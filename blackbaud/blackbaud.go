package blackbaud

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
)

type Config struct {
	Other struct {
		ApiSubscriptionKey string `json:"api_subscription_key"`
		TestApiEndpoint    string `json:"test_api_endpoint"`
		RedirectURI        string `json:"redirect_uri"`
	} `json:"other"`
	Tokens struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	} `json:"tokens"`
	SkyAppInformation struct {
		AppID     string `json:"app_id"`
		AppSecret string `json:"app_secret"`
	} `json:"sky_app_information"`
}

type BBAPIConnector struct {
	config     *Config
	configPath string
	Client     *http.Client
}

const (
	TOKEN_URL string = "https://oauth2.sky.blackbaud.com/token"
	LISTS_API string = "https://api.sky.blackbaud.com/school/v1/lists/advanced"
	HOST      string = "api.sky.blackbaud.com"
)

// Create a new API connector using an existing JSON path (MUST exist, currently we don't generate the auth stuff just yet)
// if the auth token is bad, it refreshes it
func NewBBApiConnector(configPath string) (*BBAPIConnector, error) {
	config, err := loadConfig(configPath)
	if err != nil {
		return nil, err
	}

	client := &http.Client{}
	connector := &BBAPIConnector{
		&config,
		configPath,
		client,
	}
	req, err := connector.NewRequest(http.MethodGet, config.Other.TestApiEndpoint, nil /* body */)
	if err != nil {
		return nil, err
	}
	resp, err := connector.Client.Do(req)
	if err != nil {
		return nil, err
	}
	body, _ := io.ReadAll(resp.Body)
	switch resp.StatusCode {
	case http.StatusUnauthorized:
		err = refreshToken(&config)
		if err != nil {
			return nil, err
		}
		err = saveConfig(configPath, config)
		if err != nil {
			return nil, err
		}

		return NewBBApiConnector(configPath)
	case http.StatusOK:
		return connector, nil
	}

	return nil, fmt.Errorf("unexpected response: %d, response body: %s, %v", resp.StatusCode, string(body), resp)

}

func refreshToken(config *Config) error {

	form := url.Values{}
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", config.Tokens.RefreshToken)
	form.Set("preserve_refresh_token", "true")
	form.Set("client_id", config.SkyAppInformation.AppID)
	form.Set("client_secret", config.SkyAppInformation.AppSecret)
	req, err := http.NewRequest(http.MethodPost, TOKEN_URL, strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if err != nil {
		return err
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("could not refresh auth token, response code returned: %d, body: %s", resp.StatusCode, string(body))
	}

	tokenResp := struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
	}{}

	err = json.Unmarshal(body, &tokenResp)
	if err != nil {
		return err
	}

	config.Tokens.AccessToken = tokenResp.AccessToken
	config.Tokens.RefreshToken = tokenResp.RefreshToken
	return resp.Body.Close()
}

func (b *BBAPIConnector) NewRequest(method string, url string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequest(method, url, body)

	if err != nil {
		return req, err
	}

	req.Header.Set("Bb-Api-Subscription-Key", b.config.Other.ApiSubscriptionKey)
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", b.config.Tokens.AccessToken))
	req.Header.Set("Host", HOST)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func loadConfig(configPath string) (Config, error) {
	var config Config
	f, err := os.Open(configPath)
	if err != nil {
		return config, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return config, err
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return config, err
	}
	return config, nil
}

func saveConfig(configPath string, config Config) error {
	const filePerm = 0644

	data, err := json.MarshalIndent(config, "", "    ")
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	// Write file with permissions — respected on Unix, ignored on Windows
	err = os.WriteFile(configPath, data, filePerm)
	if err != nil {
		return fmt.Errorf("failed to write config to %s: %w", configPath, err)
	}

	return nil
}
