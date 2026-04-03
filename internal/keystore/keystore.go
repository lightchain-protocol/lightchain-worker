// Package keystore manages the worker's ECDH P-256 encryption key pair.
// The key is persisted encrypted at rest using scrypt key derivation + AES-256-GCM.
package keystore

import (
	"crypto/ecdh"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	"golang.org/x/crypto/scrypt"
)

// encryptedKeyFile is the JSON format stored on disk.
type encryptedKeyFile struct {
	Version      int    `json:"version"`      // always 1
	Salt         string `json:"salt"`         // base64-encoded 16-byte random salt
	EncryptedKey string `json:"encryptedKey"` // base64-encoded AES-256-GCM ciphertext
}

const (
	scryptN   = 32768
	scryptR   = 8
	scryptP   = 1
	scryptLen = 32
	saltLen   = 16
)

// LoadOrGenerate loads an existing ECDH private key from path, or generates and persists
// a new one if path does not exist. The passphrase is used to derive the file-encryption key.
func LoadOrGenerate(path, passphrase string) (*ecdh.PrivateKey, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return generate(path, passphrase)
	}

	return load(path, passphrase)
}

// generate creates a new ECDH key pair, encrypts it, and writes it atomically to path.
func generate(path, passphrase string) (*ecdh.PrivateKey, error) {
	privKey, err := pkgcrypto.GenerateKeyPair()
	if err != nil {
		return nil, fmt.Errorf("generate ECDH key pair: %w", err)
	}

	salt := make([]byte, saltLen)
	if _, err := rand.Read(salt); err != nil {
		return nil, fmt.Errorf("generate salt: %w", err)
	}

	fileKey, err := deriveKey(passphrase, salt)
	if err != nil {
		return nil, err
	}

	ciphertext, err := pkgcrypto.Encrypt(fileKey, privKey.Bytes())
	if err != nil {
		return nil, fmt.Errorf("encrypt ECDH private key: %w", err)
	}

	kf := encryptedKeyFile{
		Version:      1,
		Salt:         base64.StdEncoding.EncodeToString(salt),
		EncryptedKey: base64.StdEncoding.EncodeToString(ciphertext),
	}
	data, err := json.Marshal(kf)
	if err != nil {
		return nil, fmt.Errorf("marshal keystore: %w", err)
	}

	if err := writeAtomic(path, data); err != nil {
		return nil, fmt.Errorf("write keystore: %w", err)
	}

	return privKey, nil
}

// load reads and decrypts an existing keystore file.
func load(path, passphrase string) (*ecdh.PrivateKey, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read keystore %s: %w", path, err)
	}

	var kf encryptedKeyFile
	if err := json.Unmarshal(data, &kf); err != nil {
		return nil, fmt.Errorf("parse keystore %s: %w", path, err)
	}

	if kf.Version != 1 {
		return nil, fmt.Errorf("unsupported keystore version %d in %s (expected 1)", kf.Version, path)
	}

	salt, err := base64.StdEncoding.DecodeString(kf.Salt)
	if err != nil {
		return nil, fmt.Errorf("decode salt: %w", err)
	}

	ciphertext, err := base64.StdEncoding.DecodeString(kf.EncryptedKey)
	if err != nil {
		return nil, fmt.Errorf("decode encrypted key: %w", err)
	}

	fileKey, err := deriveKey(passphrase, salt)
	if err != nil {
		return nil, err
	}

	privBytes, err := pkgcrypto.Decrypt(fileKey, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt ECDH private key (wrong passphrase?): %w", err)
	}

	privKey, err := ecdh.P256().NewPrivateKey(privBytes)
	if err != nil {
		return nil, fmt.Errorf("load ECDH private key from bytes: %w", err)
	}

	return privKey, nil
}

// deriveKey derives a 32-byte file-encryption key from passphrase + salt using scrypt.
func deriveKey(passphrase string, salt []byte) ([]byte, error) {
	key, err := scrypt.Key([]byte(passphrase), salt, scryptN, scryptR, scryptP, scryptLen)
	if err != nil {
		return nil, fmt.Errorf("derive file key: %w", err)
	}
	return key, nil
}

// writeAtomic writes data to path using a unique temp file + rename for crash safety.
// Using os.CreateTemp avoids race conditions when multiple callers write concurrently.
func writeAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create keystore directory: %w", err)
	}

	f, err := os.CreateTemp(dir, filepath.Base(path)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp keystore file: %w", err)
	}
	tmpPath := f.Name()

	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp keystore: %w", err)
	}
	if err := f.Chmod(0600); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("chmod temp keystore: %w", err)
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp keystore: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("rename keystore: %w", err)
	}

	return nil
}
