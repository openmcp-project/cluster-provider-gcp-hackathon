package shared

import (
	"context"
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"google.golang.org/api/option"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// GCPCredentials holds parsed GCP authentication material.
// Either JSONKey or TokenSource is set, not both.
type GCPCredentials struct {
	// JSONKey is the raw service account JSON key bytes, if credentials were provided via a JSON key file.
	JSONKey []byte
	// TokenSource is a token source derived from the credentials, suitable for use with GCP APIs.
	TokenSource oauth2.TokenSource
	// ProjectID extracted from the JSON key (may be empty for token-based credentials).
	ProjectID string
}

// ClientOptions returns google.golang.org/api/option.ClientOption values that authenticate API calls.
func (c *GCPCredentials) ClientOptions() []option.ClientOption {
	if len(c.JSONKey) > 0 {
		return []option.ClientOption{option.WithCredentialsJSON(c.JSONKey)}
	}
	return []option.ClientOption{option.WithTokenSource(c.TokenSource)}
}

// LoadCredentials reads GCP credentials from a Kubernetes Secret.
//
// The secret must contain either:
//   - key "credentials.json": a GCP service account JSON key file, or
//   - key "token": a static bearer token string.
func LoadCredentials(ctx context.Context, cl client.Client, namespace, name string) (*GCPCredentials, error) {
	s := &corev1.Secret{}
	if err := cl.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, s); err != nil {
		return nil, fmt.Errorf("error getting credentials secret '%s/%s': %w", namespace, name, err)
	}

	if raw, ok := s.Data["credentials.json"]; ok && len(raw) > 0 {
		// validate it parses as JSON
		var probe map[string]interface{}
		if err := json.Unmarshal(raw, &probe); err != nil {
			return nil, fmt.Errorf("secret '%s/%s' key 'credentials.json' is not valid JSON: %w", namespace, name, err)
		}
		// derive a token source to validate the credentials
		creds, err := google.CredentialsFromJSON(ctx, raw,
			"https://www.googleapis.com/auth/cloud-platform",
		)
		if err != nil {
			return nil, fmt.Errorf("error parsing GCP credentials from secret '%s/%s': %w", namespace, name, err)
		}
		projectID, _ := probe["project_id"].(string)
		return &GCPCredentials{
			JSONKey:     raw,
			TokenSource: creds.TokenSource,
			ProjectID:   projectID,
		}, nil
	}

	if token, ok := s.Data["token"]; ok && len(token) > 0 {
		ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: string(token)})
		return &GCPCredentials{TokenSource: ts}, nil
	}

	return nil, fmt.Errorf("secret '%s/%s' must contain key 'credentials.json' or 'token'", namespace, name)
}
