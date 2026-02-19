# Minimal repro `Custom error: 139`

This test has a prefunded wallet

```
bun i
bun run compact
bun run test
``` 

I was advised to create this issue here by a member of Midnight Foundation to track it. Personally I couldn't find where this issue comes from, so I don't know if this issue cames from the compiler-generated circuit, the wallet-sdk transaction handling, the node, etc. 

### Context & versions:

Compact compiler 0.29.0

Linux 6.18.3-arc1
AMD Ryzen 7 5700z
32gb RAM

A minimal reproduction of the issue can be found [here](https://github.com/midnames/deploy-receive-test).

### Steps to reproduce:

1- Compile a contract with `receiveUnshielded` and/or `sendUnshielded`
2- Deploy the contract
3- Call the circuit that has one of those functions.

### Actual behavior:

```
RPC-CORE: submitAndWatchExtrinsic(extrinsic: Extrinsic): ExtrinsicStatus:: 1010: Invalid Transaction: Custom error: 139
```

### Expected behavior:

Transaction to succeed.