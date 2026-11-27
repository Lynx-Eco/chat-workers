// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"errors"
	"testing"

	"github.com/slack-go/slack"
	"go.mau.fi/mautrix-slack/pkg/connector"

	"lynx.invalid/ingest/chatworker/backend"
	"lynx.invalid/ingest/chatworker/contract"
)

func initWith(settings map[string]string) *contract.Init {
	return &contract.Init{Settings: settings}
}

// TestReadConversationCount: blank or absent is 15; a decimal count from 0
// to 1000 is taken as it is; anything else refuses the start with
// bad_settings. WHY: catches a lenient parse (a typo turning every chat
// into a room, or none), the connector's -1 slipping through, and a bound
// off by one.
func TestReadConversationCount(t *testing.T) {
	for _, c := range []struct {
		in   string
		set  bool
		want int
	}{
		{"", false, 15},
		{"", true, 15},
		{"  ", true, 15},
		{"0", true, 0},
		{"1", true, 1},
		{" 40 ", true, 40},
		{"007", true, 7},
		{"1000", true, 1000},
	} {
		s := map[string]string{}
		if c.set {
			s[settingMaxInitialConversations] = c.in
		}
		got, err := readConversationCount(initWith(s))
		if err != nil || got != c.want {
			t.Errorf("%q: %d %v, want %d", c.in, got, err, c.want)
		}
	}
	for _, bad := range []string{"-1", "1001", "+5", "1e3", "4 0", "many", "0x10", "99999999999999999999", "٣"} {
		_, err := readConversationCount(initWith(map[string]string{settingMaxInitialConversations: bad}))
		var se *backend.SettingsError
		if !errors.As(err, &se) || se.Code != "bad_settings" {
			t.Errorf("%q: %v, want bad_settings", bad, err)
		}
	}
}

// TestNewConnectorConfig: the connector's config is its example with the
// worker's changes, and the templates it runs are the final ones (the
// connector parses them only when its config is read). WHY: catches a
// template string changed after the parse (the connector would still run
// the example's, " (bot)" and all), the custom emoji images left on, and
// the conversation count not reaching the connector.
func TestNewConnectorConfig(t *testing.T) {
	nc, err := newConnector(initWith(map[string]string{settingMaxInitialConversations: "40", "device_name": "ignored"}))
	if err != nil {
		t.Fatal(err)
	}
	n, ok := nc.(*slackNetwork)
	if !ok || n.SlackConnector == nil {
		t.Fatalf("connector %T", nc)
	}
	c := &n.SlackConnector.Config
	if c.Backfill.ConversationCount != 40 || c.CustomEmojiReactions || c.DMOnly || c.WorkspaceAvatarInRooms || c.MuteChannelsByDefault {
		t.Fatalf("config %+v", *c)
	}
	// The connector's own defaults are kept where the worker does not set
	// them.
	if c.ParticipantSyncCount != 5 || !c.ParticipantSyncOnlyOnCreate {
		t.Fatalf("participant sync %d %v", c.ParticipantSyncCount, c.ParticipantSyncOnlyOnCreate)
	}
	bot := c.FormatDisplayname(&connector.DisplaynameParams{User: &slack.User{Name: "deploybot", IsBot: true}})
	if bot != "deploybot" {
		t.Fatalf("bot name %q, want no suffix", bot)
	}
	person := c.FormatDisplayname(&connector.DisplaynameParams{User: &slack.User{Name: "ada", Profile: slack.UserProfile{RealName: "Ada L"}}})
	if person != "Ada L" {
		t.Fatalf("person name %q", person)
	}
	if ch := c.FormatChannelName(&connector.ChannelNameParams{Channel: &slack.Channel{GroupConversation: slack.GroupConversation{Name: "general"},
		IsChannel: true, IsGeneral: true}}); ch != "#general" {
		t.Fatalf("channel name %q", ch)
	}
	if _, err := newConnector(initWith(map[string]string{settingMaxInitialConversations: "all"})); err == nil {
		t.Fatal("a bad count was taken")
	}
}
