import { Effect } from "effect";
import { platform, rejection } from "./Platform.js";

const bytes = (hex: string) =>
  Uint8Array.from(hex.match(/../g) ?? [], (pair) => Number.parseInt(pair, 16));
const hex = (buffer: ArrayBuffer) =>
  [...new Uint8Array(buffer)]
    .map((n) => n.toString(16).padStart(2, "0"))
    .join("");
const raw = (key: CryptoKey) =>
  platform(
    () => crypto.subtle.exportKey("raw", key) as Promise<ArrayBuffer>,
  ).pipe(Effect.map(hex));

// RFC 8032 section 7.1, test 1: an empty message, fixed seed and public key.
export const ed25519 = Effect.gen(function* () {
  const publicBytes = bytes(
    "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a",
  );
  const privateBytes = bytes(
    "302e020100300506032b6570042204209d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60",
  );
  const algorithms = [
    { name: "Ed25519" },
    { name: "NODE-ED25519", namedCurve: "NODE-ED25519" },
  ];
  const keys = yield* Effect.forEach(algorithms, (algorithm) =>
    Effect.gen(function* () {
      return {
        privateKey: yield* platform(() =>
          crypto.subtle.importKey("pkcs8", privateBytes, algorithm, true, [
            "sign",
          ]),
        ),
        publicKey: yield* platform(() =>
          crypto.subtle.importKey("raw", publicBytes, algorithm, true, [
            "verify",
          ]),
        ),
      };
    }),
  );
  const signatures = yield* Effect.forEach(keys, (key, i) =>
    platform(() =>
      crypto.subtle.sign(algorithms[i]!, key.privateKey, new Uint8Array()),
    ),
  );
  const verified = yield* Effect.forEach(keys, (key, i) =>
    Effect.forEach(signatures, (signature) =>
      platform(() =>
        crypto.subtle.verify(
          algorithms[i]!,
          key.publicKey,
          signature,
          new Uint8Array(),
        ),
      ),
    ),
  );
  return {
    publicKeys: yield* Effect.forEach(keys, (key) => raw(key.publicKey)),
    signatures: signatures.map(hex),
    verified,
    tampered: yield* platform(() =>
      crypto.subtle.verify(
        "Ed25519",
        keys[0]!.publicKey,
        signatures[0]!,
        new Uint8Array([1]),
      ),
    ),
  };
});

// RFC 7748 section 6.1: Alice's private key, Bob's public key and shared secret.
export const x25519 = Effect.gen(function* () {
  const privateKey = yield* platform(() =>
    crypto.subtle.importKey(
      "pkcs8",
      bytes(
        "302e020100300506032b656e0422042077076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a",
      ),
      "X25519",
      false,
      ["deriveBits"],
    ),
  );
  const publicBytes = bytes(
    "de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f",
  );
  const publicKey = yield* platform(() =>
    crypto.subtle.importKey("raw", publicBytes, "X25519", true, []),
  );
  const lowOrder = yield* platform(() =>
    crypto.subtle.importKey("raw", new Uint8Array(32), "X25519", true, []),
  );
  const peer = { name: "X25519", public: publicKey };
  const lowPeer = { name: "X25519", public: lowOrder };
  return {
    publicKey: yield* raw(publicKey),
    shared: hex(
      yield* platform(() => crypto.subtle.deriveBits(peer, privateKey, 256)),
    ),
    lowOrder: yield* rejection(
      platform(() => crypto.subtle.deriveBits(lowPeer, privateKey, 256)),
    ),
  };
});
