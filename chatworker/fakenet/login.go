// SPDX-License-Identifier: AGPL-3.0-or-later

package fakenet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"maunium.net/go/mautrix/bridgev2"
	"maunium.net/go/mautrix/bridgev2/database"
	"maunium.net/go/mautrix/bridgev2/networkid"
	"maunium.net/go/mautrix/bridgev2/status"
)

// Login flows.
const (
	FlowPhone   = "phone"   // phone number, then a code to show and wait, then done
	FlowQR      = "qr"      // a QR that rotates twice, then done
	FlowCookies = "cookies" // a web sign-in returning one cookie, then done
	FlowFail    = "fail"    // a code field whose submission always fails
)

// ShownCode is what the phone flow shows; tests check it never reaches logs.
const ShownCode = "SHOWN-4321-8765"

// QRPayload is the n-th QR of the QR flow.
func QRPayload(n int) string { return fmt.Sprintf("fakenet://link?token=QRSECRET%d", n) }

func (c *Connector) GetLoginFlows() []bridgev2.LoginFlow {
	return []bridgev2.LoginFlow{
		{ID: FlowPhone, Name: "Phone and code", Description: "Enter your number, then confirm a code."},
		{ID: FlowQR, Name: "QR", Description: "Scan a code that changes."},
		{ID: FlowCookies, Name: "Web", Description: "Sign in on the web page."},
		{ID: FlowFail, Name: "Always fails", Description: "Ends in a failed step."},
	}
}

func (c *Connector) CreateLogin(ctx context.Context, user *bridgev2.User, flowID string) (bridgev2.LoginProcess, error) {
	fireLate()
	switch flowID {
	case FlowPhone, FlowQR, FlowCookies, FlowFail:
		return &Login{c: c, user: user, flow: flowID}, nil
	}
	return nil, fmt.Errorf("unknown flow")
}

// Login is one sign-in.
type Login struct {
	c        *Connector
	user     *bridgev2.User
	flow     string
	qr       int
	phone    string
	override *bridgev2.UserLogin
}

var (
	_ bridgev2.LoginProcessUserInput      = (*Login)(nil)
	_ bridgev2.LoginProcessDisplayAndWait = (*Login)(nil)
	_ bridgev2.LoginProcessCookies        = (*Login)(nil)
	_ bridgev2.LoginProcessWithOverride   = (*Login)(nil)
)

var errBadCode = bridgev2.RespError{ErrCode: "FI.FAKENET.BAD_CODE", Err: "bad code", StatusCode: 400}

var errFailedAfterLogin = errors.New("the sign-in failed after its login was made")

func (l *Login) Start(ctx context.Context) (*bridgev2.LoginStep, error) {
	switch l.flow {
	case FlowPhone:
		return &bridgev2.LoginStep{
			Type: bridgev2.LoginStepTypeUserInput, StepID: "fi.fakenet.phone", Instructions: "Enter your phone number",
			UserInputParams: &bridgev2.LoginUserInputParams{Fields: []bridgev2.LoginInputDataField{{
				Type: bridgev2.LoginInputFieldTypePhoneNumber, ID: "phone", Name: "Phone number",
			}}},
		}, nil
	case FlowQR:
		return l.qrStep(), nil
	case FlowCookies:
		return &bridgev2.LoginStep{
			Type: bridgev2.LoginStepTypeCookies, StepID: "fi.fakenet.web", Instructions: "Sign in on the page",
			CookiesParams: &bridgev2.LoginCookiesParams{
				URL: "https://fakenet.invalid/login", UserAgent: "Mozilla/5.0",
				Fields: []bridgev2.LoginCookieField{{ID: "session", Required: true, Sources: []bridgev2.LoginCookieFieldSource{
					{Type: bridgev2.LoginCookieTypeCookie, Name: "sessionid", CookieDomain: "fakenet.invalid"},
				}}},
				WaitForURLPattern: "^https://fakenet\\.invalid/app",
			},
		}, nil
	default:
		return &bridgev2.LoginStep{
			Type: bridgev2.LoginStepTypeUserInput, StepID: "fi.fakenet.code", Instructions: "Enter the code",
			UserInputParams: &bridgev2.LoginUserInputParams{Fields: []bridgev2.LoginInputDataField{{
				Type: bridgev2.LoginInputFieldType2FACode, ID: "code", Name: "Code",
			}}},
		}, nil
	}
}

func (l *Login) StartWithOverride(ctx context.Context, override *bridgev2.UserLogin) (*bridgev2.LoginStep, error) {
	l.override = override
	return l.Start(ctx)
}

func (l *Login) qrStep() *bridgev2.LoginStep {
	l.qr++
	return &bridgev2.LoginStep{
		Type: bridgev2.LoginStepTypeDisplayAndWait, StepID: "fi.fakenet.qr", Instructions: "Scan this code",
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{Type: bridgev2.LoginDisplayTypeQR, Data: QRPayload(l.qr)},
	}
}

func (l *Login) SubmitUserInput(ctx context.Context, input map[string]string) (*bridgev2.LoginStep, error) {
	if l.flow == FlowFail {
		return nil, errBadCode
	}
	if input["phone"] == "" {
		return nil, errors.New("phone required")
	}
	l.phone = input["phone"]
	return &bridgev2.LoginStep{
		Type: bridgev2.LoginStepTypeDisplayAndWait, StepID: "fi.fakenet.code", Instructions: "Type this code on your phone",
		DisplayAndWaitParams: &bridgev2.LoginDisplayAndWaitParams{Type: bridgev2.LoginDisplayTypeCode, Data: ShownCode},
	}, nil
}

func (l *Login) SubmitCookies(ctx context.Context, cookies map[string]string) (*bridgev2.LoginStep, error) {
	if cookies["session"] == "" {
		return nil, errors.New("session cookie required")
	}
	return l.complete(ctx)
}

func (l *Login) Wait(ctx context.Context) (*bridgev2.LoginStep, error) {
	wait := time.Duration(l.c.script.ShowWaitMs) * time.Millisecond
	if l.flow == FlowQR {
		wait = time.Duration(l.c.script.QRRotateMs) * time.Millisecond
	}
	select {
	case <-time.After(wait):
	case <-ctx.Done():
		if gate := l.c.script.StoreAfterCancelGate; gate != "" && l.flow == FlowQR {
			if _, err := os.ReadFile(gate); err == nil {
				_, _ = l.complete(context.WithoutCancel(ctx))
			}
		}
		return nil, bridgev2.ErrLoginStepCancelled
	}
	if l.flow == FlowQR && l.qr < 3 {
		return l.qrStep(), nil
	}
	return l.complete(ctx)
}

func (l *Login) complete(ctx context.Context) (*bridgev2.LoginStep, error) {
	account := l.c.script.Account
	if l.flow == FlowQR && l.c.script.QRAccount != "" {
		account = l.c.script.QRAccount
	}
	params := &bridgev2.NewLoginParams{DeleteOnConflict: true}
	if l.c.script.BypassLoadGate {
		params.LoadUserLogin = l.c.LoadUserLogin
	}
	ul, err := l.user.NewLogin(ctx, &database.UserLogin{
		ID:            networkid.UserLoginID(account),
		RemoteName:    l.c.script.AccountName,
		RemoteProfile: status.RemoteProfile{Name: l.c.script.AccountName, Phone: "+15550000000"},
		Metadata:      &LoginMeta{},
	}, params)
	if err != nil {
		return nil, err
	}
	if err := l.c.saveSession(ctx, string(ul.ID), l.phone); err != nil {
		return nil, err
	}
	l.c.record(Record{Kind: "login", Target: string(ul.ID)})
	go ul.Client.Connect(ul.Log.WithContext(l.c.br.BackgroundCtx))
	if l.c.script.FailAfterLogin {
		return nil, errFailedAfterLogin
	}
	if d := l.c.script.CompleteDelayMs; d > 0 {
		select {
		case <-time.After(time.Duration(d) * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	done := &bridgev2.LoginCompleteParams{UserLoginID: ul.ID, UserLogin: ul}
	if l.c.script.CompleteNoLoginID {
		done = &bridgev2.LoginCompleteParams{}
	}
	return &bridgev2.LoginStep{
		Type: bridgev2.LoginStepTypeComplete, StepID: "fi.fakenet.complete", Instructions: "Signed in",
		CompleteParams: done,
	}, nil
}

func (l *Login) Cancel() {}
