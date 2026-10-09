package memorystore

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// EncryptedCodec wraps another Codec and encrypts every encoded record
// with AES-256-GCM.
//
// It sits at the serialization boundary rather than inside a driver,
// so every backend (Redis, Postgres, SQLite) gets encryption at rest
// for free and no driver ever handles a key.
//
// Wire format per record:
//
//	base64( "ttae1" || nonce(12) || ciphertext||tag )
//
// The version prefix exists so the format can change later without
// guessing at legacy bytes; the base64 envelope keeps ciphertext
// storable in text columns and JSON documents alike.
type EncryptedCodec struct {
	inner   Codec
	aead    cipher.AEAD
	version byte
}

// encryptionVersion is the current envelope version.
const encryptionVersion byte = 1

// encryptionPrefix marks an encrypted record ("tt-agent encrypted v1").
var encryptionPrefix = []byte{'t', 't', 'a', 'e', encryptionVersion}

// ErrDecrypt reports a record that could not be decrypted, which
// usually means a wrong key or tampered data.
var ErrDecrypt = errors.New("memorystore: decrypt failed")

// NewEncryptedCodec builds an encrypting codec over inner.
//
// key may be any length; it is hashed to 32 bytes with SHA-256, so
// passphrases work without the caller having to pad or stretch them.
// inner may be nil, meaning plaintext JSON underneath the encryption.
func NewEncryptedCodec(key []byte, inner Codec) (*EncryptedCodec, error) {
	if len(key) == 0 {
		return nil, errors.New("memorystore: encryption key must not be empty")
	}
	if inner == nil {
		inner = JSONCodec{}
	}
	sum := sha256.Sum256(key)
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		return nil, fmt.Errorf("memorystore: cipher init: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("memorystore: gcm init: %w", err)
	}
	return &EncryptedCodec{inner: inner, aead: aead, version: encryptionVersion}, nil
}

// EncodeMessage implements Codec.
func (c *EncryptedCodec) EncodeMessage(rec MessageRecord) ([]byte, error) {
	plain, err := c.inner.EncodeMessage(rec)
	if err != nil {
		return nil, err
	}
	return c.seal(plain)
}

// DecodeMessage implements Codec, transparently reading legacy
// plaintext records so an existing dataset can be encrypted in place
// without a rewrite step.
func (c *EncryptedCodec) DecodeMessage(data []byte) (MessageRecord, error) {
	plain, err := c.open(data)
	if err != nil {
		return MessageRecord{}, err
	}
	return c.inner.DecodeMessage(plain)
}

// EncodeSummary implements Codec.
func (c *EncryptedCodec) EncodeSummary(rec SummaryRecord) ([]byte, error) {
	plain, err := c.inner.EncodeSummary(rec)
	if err != nil {
		return nil, err
	}
	return c.seal(plain)
}

// DecodeSummary implements Codec, with the same legacy tolerance as
// DecodeMessage.
func (c *EncryptedCodec) DecodeSummary(data []byte) (SummaryRecord, error) {
	plain, err := c.open(data)
	if err != nil {
		return SummaryRecord{}, err
	}
	return c.inner.DecodeSummary(plain)
}

// Unwrap exposes the wrapped codec.
func (c *EncryptedCodec) Unwrap() Codec { return c.inner }

// seal encrypts plaintext into the versioned base64 envelope.
func (c *EncryptedCodec) seal(plain []byte) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("memorystore: nonce: %w", err)
	}
	// Nonce is prepended as additional data so a ciphertext cannot be
	// recombined with a different nonce by an attacker with write
	// access to the store.
	out := make([]byte, 0, len(encryptionPrefix)+len(nonce)+len(plain)+c.aead.Overhead())
	out = append(out, encryptionPrefix...)
	out = append(out, nonce...)
	out = c.aead.Seal(out, nonce, plain, nonce)
	encoded := make([]byte, base64.StdEncoding.EncodedLen(len(out)))
	base64.StdEncoding.Encode(encoded, out)
	return encoded, nil
}

// open reverses seal.
//
// Data without the version prefix is returned as-is and decoded by the
// inner codec, which is what makes legacy plaintext readable.
func (c *EncryptedCodec) open(data []byte) ([]byte, error) {
	raw := make([]byte, base64.StdEncoding.DecodedLen(len(data)))
	n, err := base64.StdEncoding.Decode(raw, data)
	if err != nil || !hasPrefix(raw[:n], encryptionPrefix) {
		return data, nil // legacy plaintext record
	}
	raw = raw[:n]
	body := raw[len(encryptionPrefix):]
	nonceSize := c.aead.NonceSize()
	if len(body) < nonceSize+c.aead.Overhead() {
		return nil, ErrDecrypt
	}
	nonce, ciphertext := body[:nonceSize], body[nonceSize:]
	plain, err := c.aead.Open(nil, nonce, ciphertext, nonce)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDecrypt, err)
	}
	return plain, nil
}

// hasPrefix reports whether b starts with prefix.
func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i := range prefix {
		if b[i] != prefix[i] {
			return false
		}
	}
	return true
}

// JSONCodec satisfies Codec; asserted here so a signature change
// breaks the build rather than silently dropping encryption support.
var _ Codec = JSONCodec{}

// EncryptedCodec satisfies Codec.
var _ Codec = (*EncryptedCodec)(nil)

// jsonIsPlaintext is a compile-time reminder that JSON encoding is
// what the encrypted codec wraps by default.
var _ = json.Marshal
