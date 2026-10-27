# Security

## Reporting

Open a GitHub issue for anything that is not a secret, or email the address
on the repository owner's profile for a key-handling or signing problem you
would rather not post. There is no bounty.

## What this crate does with secrets

- **The private key** (`polymarket::signing::Wallet`) is parsed from hex
  into a `k256` signing key; the hex bytes are wiped as soon as the key holds
  them, and the key wipes itself when dropped. The wallet's `Debug` shows its
  address only. Nothing here reads the key from a file or the environment:
  the examples read `PRIVATE_KEY` themselves and pass it in.
- **API credentials** (`polymarket::rest::ApiCreds`): the secret and
  passphrase are wiped when dropped, `Debug` redacts them, and no error or
  log line formats them. They travel only in request headers to the CLOB URL
  the client was built with (the HTTP client follows no redirect) and in the
  login frame of the private stream.
- **Signed orders**: the signature is over the venue's EIP-712 domain for the
  exchange contract on Polygon (chain id 137). An order's hash is its venue
  ID, known before it is sent. Nothing here can sign anything but an order
  or the venue's ClobAuth message.
- **The ledger** (`accounting`) stores no secret: order IDs, amounts and
  transaction hashes only.

## What it does not do

- It never sends an order, cancel or login on its own: every request is a
  call the caller makes.
- It never retries a POST with a fresh salt: a lost answer is reported as
  such, since the order may rest.
- It never treats a missing fee or settlement as zero.

## Dependencies

`cargo audit` is not part of CI; run it before deploying. The crate pins no
dependency version beyond semver (`Cargo.lock` is committed for the
examples and tests).
