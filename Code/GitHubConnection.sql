USE ROLE TEAM3_DEVELOPER;

-- Create or replace secret
CREATE OR REPLACE SECRET github_secret
    TYPE = 'PASSWORD'
    USERNAME = 'doubleu0-0'
    PASSWORD = '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';

-- Create or replace API integration
CREATE OR REPLACE API INTEGRATION git_api_integration_team3
    API_PROVIDER = 'git_https_api'
    API_ALLOWED_PREFIXES = ('https://github.com/doubleu0-0')
    ALLOWED_AUTHENTICATION_SECRETS = (github_secret)
    ENABLED = TRUE;

-- Create or replace Git repository
CREATE OR REPLACE GIT REPOSITORY VelocityNet
    API_INTEGRATION = git_api_integration_team3
    GIT_CREDENTIALS = github_secret
    ORIGIN = 'https://github.com/doubleu0-0/VelocityNet-DS02-Team3-NotScam-';

-- Show repos added to Snowflake
SHOW GIT REPOSITORIES;

-- Show branches in the repo
SHOW GIT BRANCHES IN GIT REPOSITORY VelocityNet;

-- List files in the repository
LS @VelocityNet/branches/main;
