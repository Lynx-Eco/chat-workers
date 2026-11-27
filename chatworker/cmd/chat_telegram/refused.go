// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"errors"

	"go.mau.fi/mautrix-telegram/pkg/connector"
	"go.mau.fi/mautrix-telegram/pkg/gotd/telegram"
	"maunium.net/go/mautrix/bridgev2"

	"lynx.invalid/ingest/chatworker/backend"
)

var (
	errNotTelegram  = errors.New("not a Telegram connector and login")
	errNotConfirmed = errors.New("auth.logOut was not answered")
)

// endRefused ends at Telegram the session a refused sign-in authorised
// (backend.Hooks.EndRefused). The connector's login flows keep the new
// session (its auth key, data centre and salt) only in memory until they
// call NewLogin, which hands it over in the login's metadata; the login
// process's own client is cancelled before that call. So the session is
// logged out with a client of its own: the connector's gotd client on a
// copy of that session in memory (no session file, no update state, no
// peers: nothing of it reaches the store), connected through the
// connector's proxy setting, calling auth.logOut once. The auth key is then
// zeroed in the refused login, and logOut zeroes the key of its own copy of
// the session. Only those slices, which this code owns, are zeroed: the gotd
// client copies the key into structures of its own, which Go gives no way
// to zero, and those copies stay on the heap until they are garbage
// collected and the memory is reused. A session imported with the manual
// flow existed before the sign-in and may be in use elsewhere: it is not
// ended.
// With no key, or such a session, nothing is dialled and the result is
// backend.ErrNothingToEnd (logged as refused_session_nothing_to_end, not
// as ended). nil (refused_session_ended) only when auth.logOut was
// answered; ctx ends it (its 30 s limit, or the worker's stop: see
// backend's waitRefusedEnds) with an error.
func endRefused(ctx context.Context, inner bridgev2.NetworkConnector, login *bridgev2.UserLogin) error {
	tc, isTC := inner.(*connector.TelegramConnector)
	meta, isMeta := login.Metadata.(*connector.UserLoginMetadata)
	if !isTC || !isMeta {
		return errNotTelegram
	}
	defer clear(meta.Session.AuthKey)
	if !meta.Session.HasAuthKey() || meta.LoginMethod == connector.LoginFlowIDManual {
		return backend.ErrNothingToEnd
	}
	return logOut(ctx, tc.Config, meta.Session)
}

// logOut connects with session (a copy; the key slice it holds when it
// returns is zeroed, not the copies the client made of it: see endRefused)
// and calls auth.logOut.
func logOut(ctx context.Context, cfg connector.TelegramConfig, session connector.UserLoginSession) error {
	session.AuthKey = append([]byte(nil), session.AuthKey...)
	// The client may store a new copy (Save) while it runs.
	defer func() { clear(session.AuthKey) }()
	resolver, err := connector.GetProxyResolver(cfg.ProxyConfig)
	if err != nil {
		return err
	}
	client := telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
		Resolver:             resolver,
		CustomSessionStorage: &session,
		NoUpdates:            true,
		Device: telegram.DeviceConfig{
			DeviceModel:    cfg.DeviceInfo.DeviceModel,
			SystemVersion:  cfg.DeviceInfo.SystemVersion,
			AppVersion:     cfg.DeviceInfo.AppVersion,
			SystemLangCode: cfg.DeviceInfo.SystemLangCode,
			LangCode:       cfg.DeviceInfo.LangCode,
		},
	})
	confirmed := false
	err = client.Run(ctx, func(ctx context.Context) error {
		_, err := client.API().AuthLogOut(ctx)
		confirmed = err == nil
		return err
	})
	if err == nil && !confirmed {
		// Run returns nil when its context is cancelled (the worker's stop
		// gave up on the logout) before the call was answered: that is no
		// logout.
		err = errNotConfirmed
	}
	return err
}
