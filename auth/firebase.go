/*
 * Copyright (c) 2021. Victor Ruscitto (vrus@vrcyber.com). All rights reserved.
 */

package auth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"firebase.google.com/go/v4"
	"firebase.google.com/go/v4/auth"
)

const (
	baseURL      = "https://identitytoolkit.googleapis.com/v1"
	tokenBaseURL = "https://securetoken.googleapis.com/v1"

	contentTypeJSON = "application/json"
	contentTypeForm = "application/x-www-form-urlencoded"
)

/* Auth info from Cloud Endpoints + Firebase */
type EndpointUser struct {
	UID    string `json:"id"`
	Issuer string `json:"issuer"`
	Email  string `json:"email"`
}

/* Auth info from API Gateway */
type GatewayUser struct {
	Name          string   `json:"name"`
	Picture       string   `json:"picture"`
	Iss           string   `json:"iss"`
	Aud           string   `json:"aud"`
	AuthTime      int      `json:"auth_time"`
	UserID        string   `json:"user_id"`
	Sub           string   `json:"sub"`
	Iat           int      `json:"iat"`
	Exp           int      `json:"exp"`
	Email         string   `json:"email"`
	EmailVerified bool     `json:"email_verified"`
	Firebase      Firebase `json:"firebase"`
}

type Firebase struct {
	Identities     Identities `json:"identities"`
	SignInProvider string     `json:"sign_in_provider"`
}

type Identities struct {
	GoogleCom []string `json:"google.com"`
	Email     []string `json:"email"`
}

type FBLoginResp struct {
	UID          string `json:"localId"`
	Email        string `json:"email"`
	DisplayName  string `json:"displayName"`
	IDToken      string `json:"idToken"`
	Registered   bool   `json:"registered"`
	RefreshToken string `json:"refreshToken"`
	ExpiresIn    string `json:"expiresIn"`
}

type FBLoginError struct {
	Error FBError `json:"error"`
}

type FBError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type FBRefreshTokenResp struct {
	UID          string `json:"user_id"`
	ProjectID    string `json:"project_id"`
	ExpiresIn    string `json:"expires_in"`
	TokenType    string `json:"token_type"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
}

type FirebaseAuth struct {
	apiKey string
	client *auth.Client
}

// NewFirebaseAuth
func NewFirebaseAuth(apiKey string) (*FirebaseAuth, error) {
	fbApp, err := firebase.NewApp(context.Background(), nil)
	if err != nil {
		return nil, err
	}

	// Access auth storage from the default app
	authClient, err := fbApp.Auth(context.Background())
	if err != nil {
		return nil, err
	}

	return &FirebaseAuth{
		client: authClient,
		apiKey: apiKey,
	}, nil
}

// Login
func (f *FirebaseAuth) Login(email string, password string) (*FBLoginResp, *FBLoginError) {
	req, err := json.Marshal(map[string]interface{}{
		"email":             email,
		"password":          password,
		"returnSecureToken": true,
	})

	if err != nil {
		return nil, &FBLoginError{Error: FBError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}}
	}

	resp, status, err := submitPost(baseURL, "/accounts:signInWithPassword?key="+f.apiKey, req)
	if err != nil {
		return nil, &FBLoginError{Error: FBError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}}
	}

	if status != 200 {
		var loginError FBLoginError

		if err = json.Unmarshal(resp, &loginError); err != nil {
			return nil, &FBLoginError{Error: FBError{
				Code:    http.StatusInternalServerError,
				Message: err.Error(),
			}}
		}

		return nil, &loginError
	}

	var auth FBLoginResp

	if err = json.Unmarshal(resp, &auth); err != nil {
		return nil, &FBLoginError{Error: FBError{
			Code:    http.StatusInternalServerError,
			Message: err.Error(),
		}}
	}

	return &auth, nil
}

// RefreshToken
func (f *FirebaseAuth) RefreshToken(refreshToken string) (*FBRefreshTokenResp, error) {
	req := url.Values{}
	req.Set("grant_type", "refresh_token")
	req.Set("refresh_token", refreshToken)

	resp, status, err := submitForm(tokenBaseURL, "/token?key="+f.apiKey, &req)
	if err != nil {
		return nil, err
	}

	if status != 200 {
		if resp != nil {
			return nil, fmt.Errorf("firebase returned http status %v. body: %v", status, string(resp))
		} else {
			return nil, fmt.Errorf("firebase returned http status %v", status)
		}
	}

	var token FBRefreshTokenResp
	err = json.Unmarshal(resp, &token)

	if err != nil {
		return nil, err
	}

	return &token, nil
}

// GetRoleFromToken
func (f *FirebaseAuth) GetRoleFromToken(idToken string) (string, error) {
	token, err := f.client.VerifyIDToken(context.Background(), idToken)
	if err != nil {
		return "", err
	}

	claims := token.Claims
	if role, found := claims["role"]; found {
		return role.(string), nil
	} else {
		return "", nil
	}
}

// CreateUser
func (f *FirebaseAuth) CreateUser(email string, phone string, pwd string, name string, avatar string, verified bool, disabled bool) (string, error) {
	params := (&auth.UserToCreate{}).
		Email(email).
		EmailVerified(verified).
		//PhoneNumber(phone).
		Password(pwd).
		DisplayName(name).
		Disabled(disabled)

	if len(avatar) > 0 {
		params = params.PhotoURL(avatar)
	}
	if len(phone) > 0 {
		params = params.PhoneNumber(phone)
	}

	user, err := f.client.CreateUser(context.Background(), params)

	if err != nil {
		return "", err
	}

	return user.UID, nil

	/*
		claims := map[string]interface{}{"role": "admin"}

		err = f.client.SetCustomUserClaims(ctx, u.UID, claims)
		if err != nil {
			log.Fatalf("error setting custom claims %v\n", err)
		}
	*/
}

// UpdateUser
func (f *FirebaseAuth) UpdateUser(uid string, email string, pwd string, name string, avatar string, phone string, verified bool, disabled bool) error {
	params := (&auth.UserToUpdate{}).
		Email(email).
		EmailVerified(verified).
		PhoneNumber(phone).
		Password(pwd).
		DisplayName(name).
		PhotoURL(avatar).
		Disabled(disabled)

	_, err := f.client.UpdateUser(context.Background(), uid, params)

	if err != nil {
		return err
	}

	return nil
}

// UpdateUserEmail
func (f *FirebaseAuth) UpdateUserEmail(uid string, email string) error {
	params := (&auth.UserToUpdate{}).
		Email(email)

	_, err := f.client.UpdateUser(context.Background(), uid, params)

	if err != nil {
		return err
	}

	return nil
}

// UpdateUserPassword
func (f *FirebaseAuth) UpdateUserPassword(uid string, password string) error {
	params := (&auth.UserToUpdate{}).
		Password(password)

	_, err := f.client.UpdateUser(context.Background(), uid, params)

	if err != nil {
		return err
	}

	return nil
}

// ResetPasswordLink
func (f *FirebaseAuth) ResetPasswordLink(email string) (string, error) {
	link, err := f.client.PasswordResetLink(context.Background(), email)
	if err != nil {
		return "", err
	}

	return link, nil
}

// CheckUserExists
func (f *FirebaseAuth) CheckUserExists(email string) (bool, error) {
	if user, err := f.client.GetUserByEmail(context.Background(), email); err != nil {
		if strings.Contains(err.Error(), "no user exists") {
			return false, nil
		} else {
			return false, err
		}
	} else {
		if user == nil {
			return false, nil
		}
	}

	return true, nil
}

// submitPost
func submitPost(baseURL string, path string, data []byte) ([]byte, int, error) {
	resp, err := http.Post(baseURL+path, contentTypeJSON, bytes.NewBuffer(data))
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()

	if bodyBytes, err := ioutil.ReadAll(resp.Body); err != nil {
		return nil, 0, err
	} else {
		return bodyBytes, resp.StatusCode, nil
	}
}

// submitForm
func submitForm(baseURL string, path string, data *url.Values) ([]byte, int, error) {
	client := &http.Client{}
	r, err := http.NewRequest("POST", baseURL+path, strings.NewReader(data.Encode())) // URL-encoded payload
	if err != nil {
		return nil, 0, err
	}

	r.Header.Add("Content-Type", contentTypeForm)
	r.Header.Add("Content-Length", strconv.Itoa(len(data.Encode())))

	resp, err := client.Do(r)
	if err != nil {
		return nil, 0, err
	}

	defer resp.Body.Close()

	if bodyBytes, err := ioutil.ReadAll(resp.Body); err != nil {
		return nil, 0, err
	} else {
		return bodyBytes, resp.StatusCode, nil
	}
}
