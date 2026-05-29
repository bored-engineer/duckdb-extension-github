#include "github_extension.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

void RegisterGitHubMacros(Connection &conn) {
	conn.Query("LOAD json");

	auto run = [&](const char *sql, const char *name) {
		auto result = conn.Query(sql);
		if (result->HasError()) {
			throw InvalidInputException("Failed to register %s macro: %s", name, result->GetError());
		}
	};

	run("CREATE OR REPLACE MACRO github_repo(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo)"
	    ") _",
	    "github_repo");

	run("CREATE OR REPLACE MACRO github_user(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('public-user')) AS r "
	    "FROM github_rest('/users/' || username)"
	    ") _",
	    "github_user");

	run("CREATE OR REPLACE MACRO github_user_followers(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('simple-user')) AS r "
	    "FROM github_rest('/users/' || username || '/followers', query := {'per_page': '100'})"
	    ") _",
	    "github_user_followers");

	run("CREATE OR REPLACE MACRO github_user_gpg_keys(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('gpg-key')) AS r "
	    "FROM github_rest('/users/' || username || '/gpg_keys', query := {'per_page': '100'})"
	    ") _",
	    "github_user_gpg_keys");

	run("CREATE OR REPLACE MACRO github_user_ssh_keys(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('key')) AS r "
	    "FROM github_rest('/users/' || username || '/keys', query := {'per_page': '100'})"
	    ") _",
	    "github_user_ssh_keys");

	run("CREATE OR REPLACE MACRO github_user_ssh_signing_keys(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('ssh-signing-key')) AS r "
	    "FROM github_rest('/users/' || username || '/ssh_signing_keys', query := {'per_page': '100'})"
	    ") _",
	    "github_user_ssh_signing_keys");

	run("CREATE OR REPLACE MACRO github_org_repos("
	    "org, \"type\" := NULL, sort := NULL, direction := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('minimal-repository')) AS r "
	    "FROM github_rest('/orgs/' || org || '/repos',"
	    " query := {'per_page': '100', 'type': \"type\", 'sort': sort, 'direction': direction})"
	    ") _",
	    "github_org_repos");

	run("CREATE OR REPLACE MACRO github_org(org) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('organization-full')) AS r "
	    "FROM github_rest('/orgs/' || org)"
	    ") _",
	    "github_org");

	run("CREATE OR REPLACE MACRO github_user_orgs(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('organization-simple')) AS r "
	    "FROM github_rest('/users/' || username || '/orgs', query := {'per_page': '100'})"
	    ") _",
	    "github_user_orgs");

	run("CREATE OR REPLACE MACRO github_user_social_accounts(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('social-account')) AS r "
	    "FROM github_rest('/users/' || username || '/social_accounts', query := {'per_page': '100'})"
	    ") _",
	    "github_user_social_accounts");

	run("CREATE OR REPLACE MACRO github_repo_issues("
	    "owner, repo,"
	    " milestone := NULL, state := NULL, assignee := NULL, creator := NULL,"
	    " mentioned := NULL, labels := NULL, sort := NULL, direction := NULL, since := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues',"
	    " query := {'per_page': '100', 'milestone': milestone, 'state': state,"
	    " 'assignee': assignee, 'creator': creator, 'mentioned': mentioned,"
	    " 'labels': labels, 'sort': sort, 'direction': direction, 'since': since})"
	    ") _",
	    "github_repo_issues");

	run("CREATE OR REPLACE MACRO github_repo_issue(owner, repo, issue_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/' || issue_number)"
	    ") _",
	    "github_repo_issue");

	run("CREATE OR REPLACE MACRO github_repo_issue_labels(owner, repo, issue_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('label')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/' || issue_number || '/labels',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_issue_labels");

	run("CREATE OR REPLACE MACRO github_repo_branches(owner, repo, protected := NULL, per_page := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('short-branch')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/branches',"
	    " query := {'per_page': '100', 'protected': protected})"
	    ") _",
	    "github_repo_branches");

	run("CREATE OR REPLACE MACRO github_repo_branch(owner, repo, branch) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('branch-with-protection')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/branches/' || branch)"
	    ") _",
	    "github_repo_branch");

	run("CREATE OR REPLACE MACRO github_repo_branch_protection(owner, repo, branch) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('branch-protection')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/branches/' || branch || '/protection',"
	    " paginate := false)"
	    ") _",
	    "github_repo_branch_protection");

	run("CREATE OR REPLACE MACRO github_repo_labels(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('label')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/labels', query := {'per_page': '100'})"
	    ") _",
	    "github_repo_labels");

	run("CREATE OR REPLACE MACRO github_repo_label(owner, repo, name) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('label')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/labels/' || name)"
	    ") _",
	    "github_repo_label");

	run("CREATE OR REPLACE MACRO github_repo_issue_timeline(owner, repo, issue_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('timeline-issue-events')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/' || issue_number || '/timeline',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_issue_timeline");

	run("CREATE OR REPLACE MACRO github_repo_issue_assignees(owner, repo, issue_number) AS TABLE "
	    "SELECT s.* FROM ("
	    "SELECT json_transform(a, github_rest_type('simple-user')) AS s "
	    "FROM ("
	    "SELECT unnest(json_transform(data, '{\"assignees\":[\"JSON\"]}').assignees) AS a "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/' || issue_number,"
	    " paginate := false)"
	    ")) _",
	    "github_repo_issue_assignees");

	run("CREATE OR REPLACE MACRO github_repo_issue_comments(owner, repo, issue_number, since := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue-comment')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/' || issue_number || '/comments',"
	    " query := {'per_page': '100', 'since': since})"
	    ") _",
	    "github_repo_issue_comments");

	run("CREATE OR REPLACE MACRO github_repo_issue_comment(owner, repo, comment_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue-comment')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/comments/' || comment_id)"
	    ") _",
	    "github_repo_issue_comment");

	run("CREATE OR REPLACE MACRO github_repo_issue_comments(owner, repo, sort := NULL, direction := "
	    "NULL, since := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue-comment')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/comments',"
	    " query := {'per_page': '100', 'sort': sort, 'direction': direction, 'since': since})"
	    ") _",
	    "github_repo_issue_comments");

	run("CREATE OR REPLACE MACRO github_repo_issue_events(owner, repo, issue_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue-event')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/' || issue_number || '/events',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_issue_events");

	run("CREATE OR REPLACE MACRO github_repo_issue_event(owner, repo, event_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue-event')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/events/' || event_id)"
	    ") _",
	    "github_repo_issue_event");

	run("CREATE OR REPLACE MACRO github_repo_issue_events(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue-event')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/issues/events',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_issue_events");

	run("CREATE OR REPLACE MACRO github_teams(org) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('team')) AS r "
	    "FROM github_rest('/orgs/' || org || '/teams', query := {'per_page': '100'})"
	    ") _",
	    "github_teams");

	run("CREATE OR REPLACE MACRO github_team(org, team_slug) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('team-full')) AS r "
	    "FROM github_rest('/orgs/' || org || '/teams/' || team_slug)"
	    ") _",
	    "github_team");

	run("CREATE OR REPLACE MACRO github_team_repos(org, team_slug) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('minimal-repository')) AS r "
	    "FROM github_rest('/orgs/' || org || '/teams/' || team_slug || '/repos',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_team_repos");

	run("CREATE OR REPLACE MACRO github_team_teams(org, team_slug) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('team')) AS r "
	    "FROM github_rest('/orgs/' || org || '/teams/' || team_slug || '/teams',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_team_teams");

	run("CREATE OR REPLACE MACRO github_team_members(org, team_slug, role := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('simple-user')) AS r "
	    "FROM github_rest('/orgs/' || org || '/teams/' || team_slug || '/members',"
	    " query := {'per_page': '100', 'role': role})"
	    ") _",
	    "github_team_members");

	run("CREATE OR REPLACE MACRO github_team_member(org, team_slug, username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('team-membership')) AS r "
	    "FROM github_rest('/orgs/' || org || '/teams/' || team_slug || '/memberships/' || username)"
	    ") _",
	    "github_team_member");

	run("CREATE OR REPLACE MACRO github_security_advisories("
	    "ghsa_id := NULL, cve_id := NULL, ecosystem := NULL, severity := NULL,"
	    " cwes := NULL, is_withdrawn := NULL, affects := NULL,"
	    " published := NULL, updated := NULL, modified := NULL,"
	    " type := NULL, direction := NULL, sort := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('global-advisory')) AS r "
	    "FROM github_rest('/advisories',"
	    " query := {'per_page': '100', 'ghsa_id': ghsa_id, 'cve_id': cve_id,"
	    " 'ecosystem': ecosystem, 'severity': severity, 'cwes': cwes,"
	    " 'is_withdrawn': is_withdrawn, 'affects': affects,"
	    " 'published': published, 'updated': updated, 'modified': modified,"
	    " 'type': type, 'direction': direction, 'sort': sort})"
	    ") _",
	    "github_security_advisories");

	run("CREATE OR REPLACE MACRO github_security_advisory(ghsa_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('global-advisory')) AS r "
	    "FROM github_rest('/advisories/' || ghsa_id)"
	    ") _",
	    "github_security_advisory");

	run("CREATE OR REPLACE MACRO github_repo_security_advisories("
	    "owner, repo,"
	    " direction := NULL, sort := NULL, before := NULL, after := NULL,"
	    " ecosystem := NULL, severity := NULL, cwes := NULL,"
	    " cve_id := NULL, ghsa_id := NULL, state := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-advisory')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/security-advisories',"
	    " query := {'per_page': '100', 'direction': direction, 'sort': sort,"
	    " 'before': before, 'after': after, 'ecosystem': ecosystem,"
	    " 'severity': severity, 'cwes': cwes, 'cve_id': cve_id,"
	    " 'ghsa_id': ghsa_id, 'state': state})"
	    ") _",
	    "github_repo_security_advisories");

	run("CREATE OR REPLACE MACRO github_repo_security_advisory(owner, repo, ghsa_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-advisory')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/security-advisories/' || ghsa_id)"
	    ") _",
	    "github_repo_security_advisory");

	run("CREATE OR REPLACE MACRO github_org_secret_scanning_alerts("
	    "org, state := NULL, secret_type := NULL, resolution := NULL, sort := NULL, direction := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('secret-scanning-alert')) AS r "
	    "FROM github_rest('/orgs/' || org || '/secret-scanning/alerts',"
	    " query := {'per_page': '100', 'state': state, 'secret_type': secret_type,"
	    " 'resolution': resolution, 'sort': sort, 'direction': direction})"
	    ") _",
	    "github_org_secret_scanning_alerts");

	run("CREATE OR REPLACE MACRO github_repo_secret_scanning_alerts("
	    "owner, repo, state := NULL, secret_type := NULL, resolution := NULL, sort := NULL, direction := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('secret-scanning-alert')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/secret-scanning/alerts',"
	    " query := {'per_page': '100', 'state': state, 'secret_type': secret_type,"
	    " 'resolution': resolution, 'sort': sort, 'direction': direction})"
	    ") _",
	    "github_repo_secret_scanning_alerts");

	run("CREATE OR REPLACE MACRO github_repo_secret_scanning_alert(owner, repo, alert_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('secret-scanning-alert')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/secret-scanning/alerts/' || alert_number)"
	    ") _",
	    "github_repo_secret_scanning_alert");

	run("CREATE OR REPLACE MACRO github_repo_secret_scanning_alert_locations(owner, repo, alert_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('secret-scanning-location')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/secret-scanning/alerts/' || alert_number || "
	    "'/locations',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_secret_scanning_alert_locations");

	run("CREATE OR REPLACE MACRO github_repo_secret_scanning_scan_history(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('secret-scanning-scan-history')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/secret-scanning/scans', paginate := false)"
	    ") _",
	    "github_repo_secret_scanning_scan_history");

	run("CREATE OR REPLACE MACRO github_search_code(q, sort := NULL, direction := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('code-search-result-item')) AS r "
	    "FROM github_rest('/search/code',"
	    " query := {'per_page': '100', 'q': q, 'sort': sort, 'direction': direction},"
	    " array_key := 'items')"
	    ") _",
	    "github_search_code");

	run("CREATE OR REPLACE MACRO github_search_commits(q, sort := NULL, direction := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('commit-search-result-item')) AS r "
	    "FROM github_rest('/search/commits',"
	    " query := {'per_page': '100', 'q': q, 'sort': sort, 'direction': direction},"
	    " array_key := 'items')"
	    ") _",
	    "github_search_commits");

	run("CREATE OR REPLACE MACRO github_search_issues(q, sort := NULL, direction := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('issue-search-result-item')) AS r "
	    "FROM github_rest('/search/issues',"
	    " query := {'per_page': '100', 'q': q, 'sort': sort, 'direction': direction},"
	    " array_key := 'items')"
	    ") _",
	    "github_search_issues");

	run("CREATE OR REPLACE MACRO github_search_labels(q, repository_id, sort := NULL, direction := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('label-search-result-item')) AS r "
	    "FROM github_rest('/search/labels',"
	    " query := {'per_page': '100', 'q': q, 'repository_id': repository_id::VARCHAR,"
	    " 'sort': sort, 'direction': direction},"
	    " array_key := 'items')"
	    ") _",
	    "github_search_labels");

	run("CREATE OR REPLACE MACRO github_search_repos(q, sort := NULL, direction := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repo-search-result-item')) AS r "
	    "FROM github_rest('/search/repositories',"
	    " query := {'per_page': '100', 'q': q, 'sort': sort, 'direction': direction},"
	    " array_key := 'items')"
	    ") _",
	    "github_search_repos");

	run("CREATE OR REPLACE MACRO github_search_topics(q) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('topic-search-result-item')) AS r "
	    "FROM github_rest('/search/topics',"
	    " query := {'per_page': '100', 'q': q},"
	    " array_key := 'items')"
	    ") _",
	    "github_search_topics");

	run("CREATE OR REPLACE MACRO github_search_users(q, sort := NULL, direction := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('user-search-result-item')) AS r "
	    "FROM github_rest('/search/users',"
	    " query := {'per_page': '100', 'q': q, 'sort': sort, 'direction': direction},"
	    " array_key := 'items')"
	    ") _",
	    "github_search_users");

	run("CREATE OR REPLACE MACRO github_repo_autolinks(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('autolink')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/autolinks')"
	    ") _",
	    "github_repo_autolinks");

	run("CREATE OR REPLACE MACRO github_repo_autolink(owner, repo, autolink_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('autolink')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/autolinks/' || autolink_id)"
	    ") _",
	    "github_repo_autolink");

	run("CREATE OR REPLACE MACRO github_repo_properties(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('custom-property-value')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/properties/values')"
	    ") _",
	    "github_repo_properties");

	run("CREATE OR REPLACE MACRO github_repo_forks(owner, repo, sort := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('minimal-repository')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/forks',"
	    " query := {'per_page': '100', 'sort': sort})"
	    ") _",
	    "github_repo_forks");

	run("CREATE OR REPLACE MACRO github_repo_rulesets(owner, repo, includes_parents := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-ruleset')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/rulesets',"
	    " query := {'per_page': '100', 'includes_parents': includes_parents})"
	    ") _",
	    "github_repo_rulesets");

	run("CREATE OR REPLACE MACRO github_repo_ruleset(owner, repo, ruleset_id, includes_parents := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-ruleset')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/rulesets/' || ruleset_id,"
	    " query := {'includes_parents': includes_parents})"
	    ") _",
	    "github_repo_ruleset");

	run("CREATE OR REPLACE MACRO github_repo_ruleset_history(owner, repo, ruleset_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-ruleset')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/rulesets/' || ruleset_id || '/history',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_ruleset_history");

	run("CREATE OR REPLACE MACRO github_repo_ruleset_history_version(owner, repo, ruleset_id, version_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-ruleset')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/rulesets/' || ruleset_id || '/history/' || version_id)"
	    ") _",
	    "github_repo_ruleset_history_version");

	run("CREATE OR REPLACE MACRO github_repo_rules_for_branch(owner, repo, branch) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-rule-detailed')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/rules/branches/' || branch,"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_rules_for_branch");

	run("CREATE OR REPLACE MACRO github_org_rulesets(org, includes_parents := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-ruleset')) AS r "
	    "FROM github_rest('/orgs/' || org || '/rulesets',"
	    " query := {'per_page': '100', 'includes_parents': includes_parents})"
	    ") _",
	    "github_org_rulesets");

	run("CREATE OR REPLACE MACRO github_org_ruleset(org, ruleset_id, includes_parents := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-ruleset')) AS r "
	    "FROM github_rest('/orgs/' || org || '/rulesets/' || ruleset_id,"
	    " query := {'includes_parents': includes_parents})"
	    ") _",
	    "github_org_ruleset");

	run("CREATE OR REPLACE MACRO github_org_ruleset_history(org, ruleset_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-ruleset')) AS r "
	    "FROM github_rest('/orgs/' || org || '/rulesets/' || ruleset_id || '/history',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_org_ruleset_history");

	run("CREATE OR REPLACE MACRO github_org_ruleset_history_version(org, ruleset_id, version_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('repository-ruleset')) AS r "
	    "FROM github_rest('/orgs/' || org || '/rulesets/' || ruleset_id || '/history/' || version_id)"
	    ") _",
	    "github_org_ruleset_history_version");

	run("CREATE OR REPLACE MACRO github_repo_webhooks(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('hook')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/hooks', query := {'per_page': '100'})"
	    ") _",
	    "github_repo_webhooks");

	run("CREATE OR REPLACE MACRO github_repo_webhook(owner, repo, hook_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('hook')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/hooks/' || hook_id)"
	    ") _",
	    "github_repo_webhook");

	run("CREATE OR REPLACE MACRO github_repo_webhook_config(owner, repo, hook_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('webhook-config')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/hooks/' || hook_id || '/config')"
	    ") _",
	    "github_repo_webhook_config");

	run("CREATE OR REPLACE MACRO github_repo_webhook_deliveries(owner, repo, hook_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('hook-delivery-item')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/hooks/' || hook_id || '/deliveries',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_webhook_deliveries");

	run("CREATE OR REPLACE MACRO github_repo_webhook_delivery(owner, repo, hook_id, delivery_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('hook-delivery')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/hooks/' || hook_id || '/deliveries/' || delivery_id)"
	    ") _",
	    "github_repo_webhook_delivery");

	run("CREATE OR REPLACE MACRO github_repo_releases(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('release')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/releases', query := {'per_page': '100'})"
	    ") _",
	    "github_repo_releases");

	run("CREATE OR REPLACE MACRO github_repo_release(owner, repo, release_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('release')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/releases/' || release_id)"
	    ") _",
	    "github_repo_release");

	run("CREATE OR REPLACE MACRO github_repo_release_latest(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('release')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/releases/latest')"
	    ") _",
	    "github_repo_release_latest");

	run("CREATE OR REPLACE MACRO github_repo_release_by_tag(owner, repo, tag) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('release')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/releases/tags/' || tag)"
	    ") _",
	    "github_repo_release_by_tag");

	run("CREATE OR REPLACE MACRO github_licenses() AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('license-simple')) AS r "
	    "FROM github_rest('/licenses')"
	    ") _",
	    "github_licenses");

	run("CREATE OR REPLACE MACRO github_meta() AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('api-overview')) AS r "
	    "FROM github_rest('/meta')"
	    ") _",
	    "github_meta");

	run("CREATE OR REPLACE MACRO github_zen() AS TABLE "
	    "SELECT data AS zen "
	    "FROM github_rest('/zen', paginate := false)",
	    "github_zen");

	run("CREATE OR REPLACE MACRO github_org_installations(org) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('installation')) AS r "
	    "FROM github_rest('/orgs/' || org || '/installations',"
	    " query := {'per_page': '100'}, array_key := 'installations')"
	    ") _",
	    "github_org_installations");

	run("CREATE OR REPLACE MACRO github_org_outside_collaborators(org, filter := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('simple-user')) AS r "
	    "FROM github_rest('/orgs/' || org || '/outside_collaborators',"
	    " query := {'per_page': '100', 'filter': filter})"
	    ") _",
	    "github_org_outside_collaborators");

	run("CREATE OR REPLACE MACRO github_org_invitations(org) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('organization-invitation')) AS r "
	    "FROM github_rest('/orgs/' || org || '/invitations', query := {'per_page': '100'})"
	    ") _",
	    "github_org_invitations");

	run("CREATE OR REPLACE MACRO github_org_failed_invitations(org) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('organization-invitation')) AS r "
	    "FROM github_rest('/orgs/' || org || '/failed_invitations', query := {'per_page': '100'})"
	    ") _",
	    "github_org_failed_invitations");

	run("CREATE OR REPLACE MACRO github_org_members(org, filter := NULL, role := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('simple-user')) AS r "
	    "FROM github_rest('/orgs/' || org || '/members',"
	    " query := {'per_page': '100', 'filter': filter, 'role': role})"
	    ") _",
	    "github_org_members");

	run("CREATE OR REPLACE MACRO github_org_public_members(org) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('simple-user')) AS r "
	    "FROM github_rest('/orgs/' || org || '/public_members', query := {'per_page': '100'})"
	    ") _",
	    "github_org_public_members");

	run("CREATE OR REPLACE MACRO github_repo_pulls("
	    "owner, repo, state := NULL, head := NULL, base := NULL, sort := NULL, direction := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('pull-request-simple')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/pulls',"
	    " query := {'per_page': '100', 'state': state, 'head': head,"
	    " 'base': base, 'sort': sort, 'direction': direction})"
	    ") _",
	    "github_repo_pulls");

	run("CREATE OR REPLACE MACRO github_repo_pull(owner, repo, pull_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('pull-request')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/pulls/' || pull_number)"
	    ") _",
	    "github_repo_pull");

	run("CREATE OR REPLACE MACRO github_repo_pull_commits(owner, repo, pull_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('commit')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/pulls/' || pull_number || '/commits',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_pull_commits");

	run("CREATE OR REPLACE MACRO github_repo_pull_files(owner, repo, pull_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('diff-entry')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/pulls/' || pull_number || '/files',"
	    " query := {'per_page': '100'}, accept := 'application/vnd.github.raw+json')"
	    ") _",
	    "github_repo_pull_files");

	run("CREATE OR REPLACE MACRO github_repo_pull_reviews(owner, repo, pull_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('pull-request-review')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/pulls/' || pull_number || '/reviews',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_repo_pull_reviews");

	run("CREATE OR REPLACE MACRO github_repo_pull_review(owner, repo, pull_number, review_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('pull-request-review')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/pulls/' || pull_number || '/reviews/' || review_id)"
	    ") _",
	    "github_repo_pull_review");

	run("CREATE OR REPLACE MACRO github_repo_pull_review_comments(owner, repo, pull_number) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('pull-request-review-comment')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/pulls/' || pull_number || '/comments',"
	    " query := {'per_page': '100'},"
	    " accept := 'application/vnd.github-commitcomment.raw+json')"
	    ") _",
	    "github_repo_pull_review_comments");

	run("CREATE OR REPLACE MACRO github_ratelimit() AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('rate-limit-overview')) AS r "
	    "FROM github_rest('/rate_limit', paginate := false)"
	    ") _",
	    "github_ratelimit");

	run("CREATE OR REPLACE MACRO github_repo_activity("
	    "owner, repo,"
	    " time_period := NULL, activity_type := NULL, actor := NULL, ref := NULL, direction := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('activity')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/activity',"
	    " query := {'per_page': '100', 'time_period': time_period, 'activity_type': activity_type,"
	    " 'actor': actor, 'ref': ref, 'direction': direction})"
	    ") _",
	    "github_repo_activity");

	run("CREATE OR REPLACE MACRO github_repo_contributors(owner, repo, anon := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('contributor')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/contributors',"
	    " query := {'per_page': '100', 'anon': anon})"
	    ") _",
	    "github_repo_contributors");

	run("CREATE OR REPLACE MACRO github_repo_codeowners_errors(owner, repo, ref := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, '{\"column\":\"INT64\",\"kind\":\"STRING\",\"line\":\"INT64\","
	    "\"message\":\"STRING\",\"path\":\"STRING\",\"source\":\"STRING\",\"suggestion\":\"STRING\"}') AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/codeowners/errors',"
	    " query := {'ref': ref}, paginate := false, array_key := 'errors')"
	    ") _",
	    "github_repo_codeowners_errors");

	run("CREATE OR REPLACE MACRO github_repo_languages(owner, repo) AS TABLE "
	    "SELECT e.key AS language, e.value AS bytes "
	    "FROM ("
	    "SELECT unnest(map_entries(data::MAP(VARCHAR, UBIGINT))) AS e "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/languages', paginate := false)"
	    ") _",
	    "github_repo_languages");

	run("CREATE OR REPLACE MACRO github_repo_tags(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('tag')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/tags', query := {'per_page': '100'})"
	    ") _",
	    "github_repo_tags");

	run("CREATE OR REPLACE MACRO github_repo_teams(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('team')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/teams', query := {'per_page': '100'})"
	    ") _",
	    "github_repo_teams");

	run("CREATE OR REPLACE MACRO github_user_repos("
	    "username, \"type\" := NULL, sort := NULL, direction := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('minimal-repository')) AS r "
	    "FROM github_rest('/users/' || username || '/repos',"
	    " query := {'per_page': '100', 'type': \"type\", 'sort': sort, 'direction': direction})"
	    ") _",
	    "github_user_repos");

	run("CREATE OR REPLACE MACRO github_org_events(org) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('event')) AS r "
	    "FROM github_rest('/orgs/' || org || '/events', query := {'per_page': '100'})"
	    ") _",
	    "github_org_events");

	run("CREATE OR REPLACE MACRO github_network_events(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('event')) AS r "
	    "FROM github_rest('/networks/' || owner || '/' || repo || '/events', query := {'per_page': '100'})"
	    ") _",
	    "github_network_events");

	run("CREATE OR REPLACE MACRO github_repo_events(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('event')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/events', query := {'per_page': '100'})"
	    ") _",
	    "github_repo_events");

	run("CREATE OR REPLACE MACRO github_user_events(username) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('event')) AS r "
	    "FROM github_rest('/users/' || username || '/events', query := {'per_page': '100'})"
	    ") _",
	    "github_user_events");

	run("CREATE OR REPLACE MACRO github_app(app_slug) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('integration')) AS r "
	    "FROM github_rest('/apps/' || app_slug)"
	    ") _",
	    "github_app");

	run("CREATE OR REPLACE MACRO github_commits("
	    "owner, repo,"
	    " sha := NULL, path := NULL, author := NULL, committer := NULL, since := NULL, until := NULL"
	    ") AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('commit')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/commits',"
	    " query := {'per_page': '100', 'sha': sha, 'path': path,"
	    " 'author': author, 'committer': committer, 'since': since, 'until': until})"
	    ") _",
	    "github_commits");

	run("CREATE OR REPLACE MACRO github_commit(owner, repo, ref) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('commit')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/commits/' || ref)"
	    ") _",
	    "github_commit");

	run("CREATE OR REPLACE MACRO github_commit_pulls(owner, repo, commit_sha) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('pull-request-simple')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/commits/' || commit_sha || '/pulls',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_commit_pulls");

	run("CREATE OR REPLACE MACRO github_commit_comments(owner, repo, commit_sha) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('commit-comment')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/commits/' || commit_sha || '/comments',"
	    " query := {'per_page': '100'},"
	    " accept := 'application/vnd.github-commitcomment.raw+json')"
	    ") _",
	    "github_commit_comments");

	run("CREATE OR REPLACE MACRO github_repo_commit_comments(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('commit-comment')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/comments',"
	    " query := {'per_page': '100'},"
	    " accept := 'application/vnd.github-commitcomment.raw+json')"
	    ") _",
	    "github_repo_commit_comments");

	run("CREATE OR REPLACE MACRO github_commit_status(owner, repo, ref) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('combined-commit-status')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/commits/' || ref || '/status',"
	    " paginate := false)"
	    ") _",
	    "github_commit_status");

	run("CREATE OR REPLACE MACRO github_commit_statuses(owner, repo, ref) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('status')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/commits/' || ref || '/statuses',"
	    " query := {'per_page': '100'})"
	    ") _",
	    "github_commit_statuses");

	run("CREATE OR REPLACE MACRO github_repo_deploy_keys(owner, repo) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('deploy-key')) AS r "
	    "FROM github_rest('/repos/' || owner || '/' || repo || '/keys', query := {'per_page': '100'})"
	    ") _",
	    "github_repo_deploy_keys");

	run("CREATE OR REPLACE MACRO github_gist(gist_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, replace(github_rest_type('gist-simple'), '{}', '\"JSON\"')) AS r "
	    "FROM github_rest('/gists/' || gist_id)"
	    ") _",
	    "github_gist");

	run("CREATE OR REPLACE MACRO github_user_gists(username, since := NULL) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, replace(github_rest_type('gist-simple'), '{}', '\"JSON\"')) AS r "
	    "FROM github_rest('/users/' || username || '/gists',"
	    " query := {'per_page': '100', 'since': since})"
	    ") _",
	    "github_user_gists");

	run("CREATE OR REPLACE MACRO github_gist_forks(gist_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, replace(github_rest_type('gist-simple'), '{}', '\"JSON\"')) AS r "
	    "FROM github_rest('/gists/' || gist_id || '/forks', query := {'per_page': '100'})"
	    ") _",
	    "github_gist_forks");

	run("CREATE OR REPLACE MACRO github_gist_commits(gist_id) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, github_rest_type('gist-commit')) AS r "
	    "FROM github_rest('/gists/' || gist_id || '/commits', query := {'per_page': '100'})"
	    ") _",
	    "github_gist_commits");

	run("CREATE OR REPLACE MACRO github_gist_revision(gist_id, sha) AS TABLE "
	    "SELECT r.* FROM ("
	    "SELECT json_transform(data, replace(github_rest_type('gist-simple'), '{}', '\"JSON\"')) AS r "
	    "FROM github_rest('/gists/' || gist_id || '/' || sha)"
	    ") _",
	    "github_gist_revision");
}

} // namespace duckdb
