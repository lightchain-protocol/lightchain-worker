package keystore

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"

	pkgcrypto "github.com/lightchain/pkg/crypto"
	"golang.org/x/crypto/scrypt"
)

// sessionKeyFile is the JSON format for the encrypted session key store.
type sessionKeyFile struct {
	Version int    `json:"version"`
	Salt    string `json:"salt"`
	Data    string `json:"data"`
}

// SessionKeyStore provides thread-safe in-memory caching of session keys,
// backed by an encrypted file for persistence across restarts.
type SessionKeyStore struct {
	mu       sync.RWMutex
	keys     map[uint64][]byte
	filePath string
	fileKey  []byte
	salt     []byte
}

// NewSessionKeyStore creates or loads a session key store.
// The passphrase is used to derive the file encryption key via scrypt.
func NewSessionKeyStore(filePath, passphrase string) (*SessionKeyStore, error) {
	s := &SessionKeyStore{
		keys:     make(map[uint64][]byte),
		filePath: filePath,
	}

	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// New store — generate salt and derive key
		salt := make([]byte, saltLen)
		if _, err := rand.Read(salt); err != nil {
			return nil, fmt.Errorf("generate session store salt: %w", err)
		}
		s.salt = salt

		fileKey, err := scrypt.Key([]byte(passphrase), salt, scryptN, scryptR, scryptP, scryptLen)
		if err != nil {
			return nil, fmt.Errorf("derive session store key: %w", err)
		}
		s.fileKey = fileKey

		// Persist empty store
		if err := s.persist(); err != nil {
			return nil, fmt.Errorf("persist initial session store: %w", err)
		}

		return s, nil
	}

	// Load existing store
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("read session store %s: %w", filePath, err)
	}

	var sf sessionKeyFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, fmt.Errorf("parse session store %s: %w", filePath, err)
	}
	if sf.Version != 1 {
		return nil, fmt.Errorf("unsupported session store version %d (expected 1)", sf.Version)
	}

	salt, err := base64.StdEncoding.DecodeString(sf.Salt)
	if err != nil {
		return nil, fmt.Errorf("decode session store salt: %w", err)
	}
	s.salt = salt

	fileKey, err := scrypt.Key([]byte(passphrase), salt, scryptN, scryptR, scryptP, scryptLen)
	if err != nil {
		return nil, fmt.Errorf("derive session store key: %w", err)
	}
	s.fileKey = fileKey

	// Decrypt data
	ciphertext, err := base64.StdEncoding.DecodeString(sf.Data)
	if err != nil {
		return nil, fmt.Errorf("decode session store data: %w", err)
	}

	plaintext, err := pkgcrypto.Decrypt(fileKey, ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decrypt session store (wrong passphrase?): %w", err)
	}

	// Unmarshal the key map — stored as map[string]string (JSON doesn't support uint64 keys)
	var rawMap map[string]string
	if err := json.Unmarshal(plaintext, &rawMap); err != nil {
		return nil, fmt.Errorf("unmarshal session keys: %w", err)
	}

	for k, v := range rawMap {
		id, err := strconv.ParseUint(k, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse session ID %q: %w", k, err)
		}
		keyBytes, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("decode session key for ID %d: %w", id, err)
		}
		s.keys[id] = keyBytes
	}

	return s, nil
}

// StoreKey adds or updates a session key and persists to disk.
func (s *SessionKeyStore) StoreKey(sessionID uint64, key []byte) error {
	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.keys[sessionID] = keyCopy
	return s.persist()
}

// GetKey returns a copy of the session key for the given ID.
// Returns an error if the key is not found.
func (s *SessionKeyStore) GetKey(sessionID uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key, ok := s.keys[sessionID]
	if !ok {
		return nil, fmt.Errorf("session key not found for session %d", sessionID)
	}

	keyCopy := make([]byte, len(key))
	copy(keyCopy, key)
	return keyCopy, nil
}

// RemoveKey zeroes and removes a session key, then persists.
func (s *SessionKeyStore) RemoveKey(sessionID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if key, ok := s.keys[sessionID]; ok {
		// Zero the key material before removing
		for i := range key {
			key[i] = 0
		}
		delete(s.keys, sessionID)
	}

	return s.persist()
}

// ZeroAll zeroes all session keys in memory and persists the empty store. Call during shutdown.
func (s *SessionKeyStore) ZeroAll() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for id, key := range s.keys {
		for i := range key {
			key[i] = 0
		}
		delete(s.keys, id)
	}

	persistErr := s.persist()

	for i := range s.fileKey {
		s.fileKey[i] = 0
	}

	return persistErr
}

// persist writes the current key map to the encrypted file.
// Caller must hold s.mu (write lock).
func (s *SessionKeyStore) persist() error {
	// Serialize key map as map[string]string for JSON compatibility
	rawMap := make(map[string]string, len(s.keys))
	for id, key := range s.keys {
		rawMap[strconv.FormatUint(id, 10)] = base64.StdEncoding.EncodeToString(key)
	}

	plaintext, err := json.Marshal(rawMap)
	if err != nil {
		return fmt.Errorf("marshal session keys: %w", err)
	}

	ciphertext, err := pkgcrypto.Encrypt(s.fileKey, plaintext)
	if err != nil {
		return fmt.Errorf("encrypt session keys: %w", err)
	}

	sf := sessionKeyFile{
		Version: 1,
		Salt:    base64.StdEncoding.EncodeToString(s.salt),
		Data:    base64.StdEncoding.EncodeToString(ciphertext),
	}

	data, err := json.Marshal(sf)
	if err != nil {
		return fmt.Errorf("marshal session store: %w", err)
	}

	return writeAtomic(s.filePath, data)
}
