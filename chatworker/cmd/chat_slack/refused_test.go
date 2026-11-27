// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-slack/pkg/connector"
	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
)

// TestRefusedEndOnlyForEmailSessions: a refused login's session is signed
// out only when the email flow made it from a FIELDS step and the token is
// a user's client token; never for the token flow (the user's own browser
// session), the email flow's web step
// (single sign-on, maybe a session the web view had), a token that is not a
// user's session, no submission, or a login with no token. WHY: catches a
// refusal that signs the user's browser out, or a bot's token, and one that
// leaves the session the email sign-in opened.
func TestRefusedEndOnlyForEmailSessions(t *testing.T) {
	email := connector.LoginFlowIDEmail
	for _, c := range []struct {
		name  string
		s     *submission
		token string
		want  bool
	}{
		{"email code/workspace/2FA, xoxc", &submission{flow: email, fields: true}, "xoxc-1", true},
		{"email, xoxs", &submission{flow: email, fields: true}, "xoxs-1", true},
		{"email web step (SSO)", &submission{flow: email}, "xoxc-1", false},
		{"token flow", &submission{flow: connector.LoginFlowIDAuthToken}, "xoxc-1", false},
		{"email, bot token", &submission{flow: email, fields: true}, "xoxb-1", false},
		{"email, no token", &submission{flow: email, fields: true}, "", false},
		{"no submission", nil, "xoxc-1", false},
	} {
		if got := refusedEnd(c.s, refusedLogin(c.token), "") != nil; got != c.want {
			t.Errorf("%s: end %v, want %v", c.name, got, c.want)
		}
	}
	if refusedEnd(&submission{flow: email, fields: true}, nil, "") != nil {
		t.Error("an end for no login")
	}
	if refusedEnd(&submission{flow: email, fields: true}, &bridgev2.UserLogin{UserLogin: &database.UserLogin{}}, "") != nil {
		t.Error("an end for a login with no metadata")
	}
}

// TestDropRefusedNotesRefusal: the hook marks the running submission as
// refused, deletes nothing (no error), and gives no end for a connector
// that is not the worker's or with no submission running.
func TestDropRefusedNotesRefusal(t *testing.T) {
	n := newSlackNetwork(&connector.SlackConnector{})
	if end, err := dropRefused(context.Background(), n, refusedLogin("xoxc-1")); end != nil || err != nil {
		t.Fatalf("no submission: %v %v", end != nil, err)
	}
	s := n.begin(connector.LoginFlowIDEmail, true)
	end, err := dropRefused(context.Background(), n, refusedLogin("xoxc-1"))
	if err != nil || end == nil {
		t.Fatalf("email: %v %v", end != nil, err)
	}
	if !n.end(s) {
		t.Fatal("the refusal was not noted")
	}
	if end, err := dropRefused(context.Background(), &connector.SlackConnector{}, refusedLogin("xoxc-1")); end != nil || err != nil {
		t.Fatalf("another connector: %v %v", end != nil, err)
	}
}

// signoutServer answers auth.signout with body, and records what came.
type signoutServer struct {
	*httptest.Server
	mu      sync.Mutex
	paths   []string
	tokens  []string
	cookies []string
}

func newSignoutServer(t *testing.T, status int, body string) *signoutServer {
	s := &signoutServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		s.mu.Lock()
		s.paths = append(s.paths, r.URL.Path)
		s.tokens = append(s.tokens, r.PostForm.Get("token"))
		if c, err := r.Cookie("d"); err == nil {
			s.cookies = append(s.cookies, c.Value)
		}
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

// TestSignOut: auth.signout is called once, with the refused token and its
// "d" cookie; Slack's ok, and its answers that the token no longer works,
// are the session ended; any other answer, a server error, a closed server
// or a context that ended is not. WHY: catches a sign-out sent with the
// wrong token (it would end the connection's own session, or nothing), sent
// without the cookie (Slack refuses an xoxc- token without it), and an
// outcome logged as ended when Slack did not end it.
func TestSignOut(t *testing.T) {
	for _, c := range []struct {
		name   string
		status int
		body   string
		ended  bool
	}{
		{"ok", 200, `{"ok":true}`, true},
		{"invalid_auth", 200, `{"ok":false,"error":"invalid_auth"}`, true},
		{"token_revoked", 200, `{"ok":false,"error":"token_revoked"}`, true},
		{"other error", 200, `{"ok":false,"error":"fatal_error"}`, false},
		{"server error", 500, `oops`, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			srv := newSignoutServer(t, c.status, c.body)
			end := refusedEnd(&submission{flow: connector.LoginFlowIDEmail, fields: true}, refusedLogin("xoxc-refused"), srv.URL+"/api/")
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			err := end(ctx)
			if (err == nil) != c.ended {
				t.Fatalf("ended %v (%v), want %v", err == nil, err, c.ended)
			}
			srv.mu.Lock()
			defer srv.mu.Unlock()
			if len(srv.paths) != 1 || !strings.HasSuffix(srv.paths[0], "/auth.signout") || srv.tokens[0] != "xoxc-refused" ||
				len(srv.cookies) != 1 || srv.cookies[0] != "xoxd-c" {
				t.Fatalf("calls %v tokens %v cookies %v", srv.paths, srv.tokens, srv.cookies)
			}
		})
	}
	srv := newSignoutServer(t, 200, `{"ok":true}`)
	url := srv.URL + "/api/"
	srv.Close()
	if err := signOut(context.Background(), url, "xoxc-1", ""); err == nil {
		t.Fatal("a closed server counted as ended")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	live := newSignoutServer(t, 200, `{"ok":true}`)
	if err := signOut(ctx, live.URL+"/api/", "xoxc-1", ""); err == nil {
		t.Fatal("an ended context counted as ended")
	}
}
