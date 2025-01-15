use role training_role;

create or replace secret github_secret
    type = password
    username = 'doubleu0-0' 
    password = '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'; 

create or replace api integration git_api_integration_team3
    api_provider = git_https_api
    api_allowed_prefixes = ('https://github.com/doubleu0-0') 


    allowed_authentication_secrets = (github_secret)
    enabled = true;

create or replace git repository VelocityNet
    api_integration = git_api_integration
    git_credentials = github_secret
    origin = 'https://github.com/doubleu0-0/VelocityNet-DS02-Team3-NotScam-';


-- Show repos added to snowflake.
show git repositories;

-- Show branches in the repo.
show git branches in git repository VelocityNet;

-- List files.
ls @VelocityNet/branches/main;
