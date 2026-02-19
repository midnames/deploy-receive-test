import * as ledger from "@midnight-ntwrk/ledger-v7";
import { nativeToken } from "@midnight-ntwrk/ledger-v7";
import { type MidnightProvider, type WalletProvider } from "@midnight-ntwrk/midnight-js-types";
import { setNetworkId } from "@midnight-ntwrk/midnight-js-network-id";
import { NetworkId } from "@midnight-ntwrk/wallet-sdk-abstractions";
import { httpClientProofProvider } from "@midnight-ntwrk/midnight-js-http-client-proof-provider";
import { indexerPublicDataProvider } from "@midnight-ntwrk/midnight-js-indexer-public-data-provider";
import { NodeZkConfigProvider } from "@midnight-ntwrk/midnight-js-node-zk-config-provider";
import { levelPrivateStateProvider } from "@midnight-ntwrk/midnight-js-level-private-state-provider";
import { deployContract } from "@midnight-ntwrk/midnight-js-contracts";
import { HDWallet, Roles } from "@midnight-ntwrk/wallet-sdk-hd";
import { WalletFacade } from "@midnight-ntwrk/wallet-sdk-facade";
import { ShieldedWallet } from "@midnight-ntwrk/wallet-sdk-shielded";
import { DustWallet } from "@midnight-ntwrk/wallet-sdk-dust-wallet";
import { createKeystore, InMemoryTransactionHistoryStorage, PublicKey as UnshieldedPublicKey, UnshieldedWallet } from "@midnight-ntwrk/wallet-sdk-unshielded-wallet";
import { CompiledContract } from "@midnight-ntwrk/compact-js";
import { Contract as PongContract } from "../managed/pong/contract/index.js";
import { witnesses } from "../witnesses.js";
import * as Rx from "rxjs";
import * as path from "node:path";
import { WebSocket } from "ws";
import { Buffer } from "buffer";

globalThis.WebSocket = WebSocket as unknown as typeof globalThis.WebSocket;

// Config
const config = {
  indexer: "https://indexer.preprod.midnight.network/api/v3/graphql",
  indexerWS: "wss://indexer.preprod.midnight.network/api/v3/graphql/ws",
  node: "wss://rpc.preprod.midnight.network",
  proofServer: "http://127.0.0.1:6300",
  networkId: "preprod" as NetworkId.NetworkId,
};
setNetworkId(config.networkId);

const SEED = "557beb4d4bd5c88948712fd375b20f44ed9f38ade5e6ee8c27ece84d26de1640";
const zkConfigPath = path.resolve(import.meta.dirname, "..", "managed", "pong");

// Wallet context type
interface WalletContext {
  wallet: WalletFacade;
  shieldedSecretKeys: ledger.ZswapSecretKeys;
  dustSecretKey: ledger.DustSecretKey;
  unshieldedKeystore: any;
}

async function initWallet(): Promise<WalletContext> {
  const hdWallet = HDWallet.fromSeed(Buffer.from(SEED, "hex"));
  if (hdWallet.type !== "seedOk") throw new Error("Failed to init HDWallet");

  const derivation = hdWallet.hdWallet.selectAccount(0)
    .selectRoles([Roles.Zswap, Roles.NightExternal, Roles.Dust])
    .deriveKeysAt(0);
  if (derivation.type !== "keysDerived") throw new Error("Failed to derive keys");
  hdWallet.hdWallet.clear();

  const shieldedSecretKeys = ledger.ZswapSecretKeys.fromSeed(derivation.keys[Roles.Zswap]);
  const dustSecretKey = ledger.DustSecretKey.fromSeed(derivation.keys[Roles.Dust]);
  const unshieldedKeystore = createKeystore(derivation.keys[Roles.NightExternal], config.networkId);

  const walletConfig = {
    networkId: config.networkId,
    costParameters: { additionalFeeOverhead: 1_000_000_000n, feeBlocksMargin: 5 },
    relayURL: new URL(config.node),
    provingServerUrl: new URL(config.proofServer),
    indexerClientConnection: { indexerHttpUrl: config.indexer, indexerWsUrl: config.indexerWS },
    indexerUrl: config.indexerWS,
  };

  const wallet = new WalletFacade(
    ShieldedWallet(walletConfig).startWithSecretKeys(shieldedSecretKeys),
    UnshieldedWallet({ ...walletConfig, txHistoryStorage: new InMemoryTransactionHistoryStorage() })
      .startWithPublicKey(UnshieldedPublicKey.fromKeyStore(unshieldedKeystore)),
    DustWallet(walletConfig).startWithSecretKey(dustSecretKey, ledger.LedgerParameters.initialParameters().dust),
  );
  await wallet.start(shieldedSecretKeys, dustSecretKey);

  return { wallet, shieldedSecretKeys, dustSecretKey, unshieldedKeystore };
}

async function waitForFunds(wallet: WalletFacade) {
  await Rx.firstValueFrom(wallet.state().pipe(
    Rx.throttleTime(5000),
    Rx.tap(s => console.log(`Synced: ${s.isSynced}, Balance: ${s.unshielded?.balances[nativeToken().raw] ?? 0n}`)),
    Rx.filter(s => s.isSynced && (s.unshielded?.balances[nativeToken().raw] ?? 0n) > 0n),
  ));
}

async function registerDust(ctx: WalletContext) {
  const state = await Rx.firstValueFrom(ctx.wallet.state().pipe(Rx.filter(s => s.isSynced)));
  const unregistered = state.unshielded?.availableCoins.filter(c => !c.meta.registeredForDustGeneration) ?? [];
  if (unregistered.length === 0) return;

  console.log(`Registering ${unregistered.length} UTXOs for dust...`);
  const recipe = await ctx.wallet.registerNightUtxosForDustGeneration(
    unregistered, ctx.unshieldedKeystore.getPublicKey(),
    (p) => ctx.unshieldedKeystore.signData(p)
  );
  const tx = await ctx.wallet.finalizeTransaction(recipe.transaction);
  const txId = await ctx.wallet.submitTransaction(tx);
  console.log(`Dust registration tx: ${txId}`);

  await Rx.firstValueFrom(ctx.wallet.state().pipe(
    Rx.throttleTime(5000),
    Rx.filter(s => (s.dust?.walletBalance(new Date()) ?? 0n) > 0n),
  ));
}

function createProviders(ctx: WalletContext) {
  const zkConfig = new NodeZkConfigProvider<"pong">(zkConfigPath);

  const walletAndMidnightProvider = {
    getCoinPublicKey: () => ctx.shieldedSecretKeys.coinPublicKey as unknown as ledger.CoinPublicKey,
    getEncryptionPublicKey: () => ctx.shieldedSecretKeys.encryptionPublicKey as unknown as ledger.EncPublicKey,
    async balanceTx(tx: any, ttl?: Date) {
      const bound = tx.bind();
      const recipe = await ctx.wallet.balanceFinalizedTransaction(bound,
        { shieldedSecretKeys: ctx.shieldedSecretKeys, dustSecretKey: ctx.dustSecretKey },
        { ttl: ttl ?? new Date(Date.now() + 30 * 60 * 1000) }
      );
      const signed = await ctx.wallet.signRecipe(recipe, (p: any) => ctx.unshieldedKeystore.signData(p));
      return ctx.wallet.finalizeRecipe(signed);
    },
    submitTx: (tx: ledger.FinalizedTransaction) => ctx.wallet.submitTransaction(tx),
  };

  return {
    privateStateProvider: levelPrivateStateProvider<"pongPrivateState">({
      privateStateStoreName: "pong-private-state",
      walletProvider: walletAndMidnightProvider,
    }),
    publicDataProvider: indexerPublicDataProvider(config.indexer, config.indexerWS),
    zkConfigProvider: zkConfig,
    proofProvider: httpClientProofProvider(config.proofServer, zkConfig),
    walletProvider: walletAndMidnightProvider as WalletProvider & MidnightProvider,
    midnightProvider: walletAndMidnightProvider as MidnightProvider,
  };
}

// Pong contract instance
const pongContract = CompiledContract.make("pong", PongContract).pipe(
  CompiledContract.withWitnesses(witnesses),
  CompiledContract.withCompiledFileAssets(zkConfigPath),
);

async function main() {
  console.log("Initializing wallet...");
  const ctx = await initWallet();
  console.log(`Address: ${ctx.unshieldedKeystore.getBech32Address().asString()}`);

  console.log("Waiting for funds...");
  await waitForFunds(ctx.wallet);
  await registerDust(ctx);

  const providers = createProviders(ctx);

  // Get wallet's unshielded address bytes
  const walletPubKey = ctx.unshieldedKeystore.getPublicKey();
  const walletAddress = ledger.addressFromKey(walletPubKey);
  const deployerBytes = new Uint8Array(Buffer.from(walletAddress.toString().replace("0x", ""), "hex"));

  // Deploy pong contract
  console.log("Deploying pong contract...");
  const deployed = await deployContract(providers, {
    compiledContract: pongContract,
    privateStateId: "pongPrivateState",
    initialPrivateState: { phantom: false },
    args: [
      new Uint8Array(Buffer.from(nativeToken().raw.toString().replace("0x", ""), "hex")),
      1n,
      deployerBytes,
    ],
  });
  console.log(`Deployed at: ${deployed.deployTxData.public.contractAddress}`);

  // Call pong() in the circuit
  console.log("Calling pong()...");
  const result = await deployed.callTx.pong();
  console.log(`pong() tx status: ${result.public.status}`);

  await ctx.wallet.stop();
}

main().catch(console.error);
