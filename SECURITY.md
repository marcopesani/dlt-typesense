# Security Policy

## Supported versions

This project is pre-alpha. Security fixes target the `main` branch only until a stable release is published.

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Email the maintainers at **marco.pesani@gmail.com** with:

- A description of the issue
- Steps to reproduce (if possible)
- Impact assessment
- Any suggested fix

We will acknowledge receipt within a few business days and work with you on a coordinated disclosure.

## Secrets

Never commit Typesense API keys, `.dlt/secrets.toml`, or production credentials. Use environment variables or local secrets files listed in `.gitignore`.
