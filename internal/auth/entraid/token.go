// Package entraid provides Entra ID (Azure AD) authentication for godis.
// Adapted from github.com/redis/go-redis-entraid for both server-side
// token validation and client-side credential acquisition.
package entraid

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"time"
)

// TokenClaims represents JWT claims from an Entra ID access token.
type TokenClaims struct {
	Audience  string   `json:"aud"`
	Issuer    string   `json:"iss"`
	Subject   string   `json:"sub"`
	TenantID  string   `json:"tid"`
	AppID     string   `json:"appid"`
	Expiry    int64    `json:"exp"`
	NotBefore int64    `json:"nbf"`
}

// Validator validates Entra ID access tokens via JWKS.
type Validator struct {
	tenantID   string
	appID      string
	issuer     string
	jwksURL    string
	jwksCache  *jwksCache
	httpClient *http.Client
}

type jwksCache struct {
	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	expiresAt time.Time
}

// NewValidator creates an Entra ID token validator.
func NewValidator(tenantID, appID string) *Validator {
	return &Validator{
		tenantID: tenantID,
		appID:    appID,
		issuer:   fmt.Sprintf("https://login.microsoftonline.com/%s/v2.0", tenantID),
		jwksURL:  fmt.Sprintf("https://login.microsoftonline.com/%s/discovery/v2.0/keys", tenantID),
		jwksCache: &jwksCache{keys: make(map[string]*rsa.PublicKey)},
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// ValidateToken validates an Entra ID JWT token string.
func (v *Validator) ValidateToken(token string) (*TokenClaims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("ERR invalid token format")
	}

	headerJSON, err := joseBase64Decode(parts[0])
	if err != nil {
		return nil, fmt.Errorf("ERR invalid token header: %w", err)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return nil, fmt.Errorf("ERR invalid token header: %w", err)
	}
	if header.Alg != "RS256" {
		return nil, fmt.Errorf("ERR unsupported algorithm: %s", header.Alg)
	}

	payloadJSON, err := joseBase64Decode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("ERR invalid token payload: %w", err)
	}
	var claims TokenClaims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return nil, fmt.Errorf("ERR invalid token claims: %w", err)
	}

	expectedIssuer := fmt.Sprintf("https://login.microsoftonline.com/%s/", v.tenantID)
	if !strings.HasPrefix(claims.Issuer, expectedIssuer) && claims.Issuer != v.issuer {
		return nil, errors.New("ERR invalid token issuer")
	}
	if claims.Audience != v.appID && claims.AppID != v.appID {
		return nil, errors.New("ERR invalid token audience")
	}

	now := time.Now().Unix()
	if claims.Expiry > 0 && now > claims.Expiry {
		return nil, errors.New("ERR token expired")
	}
	if claims.NotBefore > 0 && now < claims.NotBefore {
		return nil, errors.New("ERR token not yet valid")
	}

	key, err := v.getSigningKey(header.Kid)
	if err != nil {
		return nil, fmt.Errorf("ERR failed to get signing key: %w", err)
	}

	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, errors.New("ERR invalid token signature encoding")
	}

	hash := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, hash[:], sig); err != nil {
		return nil, errors.New("ERR invalid token signature")
	}

	return &claims, nil
}

type jwk struct {
	Kty string `json:"kty"`
	N   string `json:"n"`
	E   string `json:"e"`
	Kid string `json:"kid"`
	Alg string `json:"alg"`
	Use string `json:"use"`
}

type jwksResponse struct {
	Keys []jwk `json:"keys"`
}

func (v *Validator) getSigningKey(kid string) (*rsa.PublicKey, error) {
	v.jwksCache.mu.RLock()
	key, ok := v.jwksCache.keys[kid]
	cached := ok && time.Now().Before(v.jwksCache.expiresAt)
	v.jwksCache.mu.RUnlock()
	if cached {
		return key, nil
	}

	resp, err := v.httpClient.Get(v.jwksURL)
	if err != nil {
		return nil, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer resp.Body.Close()

	var jwks jwksResponse
	if err := json.NewDecoder(resp.Body).Decode(&jwks); err != nil {
		return nil, fmt.Errorf("decode JWKS: %w", err)
	}

	v.jwksCache.mu.Lock()
	v.jwksCache.keys = make(map[string]*rsa.PublicKey)
	for _, k := range jwks.Keys {
		if k.Kty != "RSA" {
			continue
		}
		pk, err := jwkToPublicKey(&k)
		if err != nil {
			continue
		}
		v.jwksCache.keys[k.Kid] = pk
	}
	v.jwksCache.expiresAt = time.Now().Add(24 * time.Hour)
	v.jwksCache.mu.Unlock()

	v.jwksCache.mu.RLock()
	key, ok = v.jwksCache.keys[kid]
	v.jwksCache.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("signing key %q not found", kid)
	}
	return key, nil
}

func jwkToPublicKey(j *jwk) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(j.N)
	if err != nil {
		return nil, err
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(j.E)
	if err != nil {
		return nil, err
	}
	if len(eBytes) < 4 {
		padded := make([]byte, 4)
		copy(padded[4-len(eBytes):], eBytes)
		eBytes = padded
	}
	return &rsa.PublicKey{
		N: new(big.Int).SetBytes(nBytes),
		E: int(binary.BigEndian.Uint32(eBytes)),
	}, nil
}

func joseBase64Decode(s string) ([]byte, error) {
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return base64.StdEncoding.DecodeString(s)
}

// CredentialsProvider acquires and refreshes Entra ID tokens.
type CredentialsProvider interface {
	GetToken() (string, error)
}
