# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

The security of FeOx Server is important. If you believe you have found a security vulnerability, please report it to me as described below.

**Please do NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to [dev@mehran.dk]. You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure I received your original message.

Please include the following information in your report:
- Type of issue
- Full paths of source file(s) related to the manifestation of the issue
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

## Security Considerations

### Network Security
- FeOx Server binds to localhost (127.0.0.1) by default
- Use `--bind 0.0.0.0` with caution in production environments
- No built-in authentication or TLS support yet - use a reverse proxy if needed

### Memory Safety
- Written in Rust with memory safety guarantees
- Uses lock-free data structures to prevent race conditions
- Bounds checking on all buffer operations

### Known Limitations
- No authentication mechanism (all clients have full access)
- No encryption for data in transit or at rest
- No command filtering or ACL support
- Write-behind buffering may result in data loss on crash (see README for details)

## Preferred Languages

I prefer all communications to be in English.

## Policy

- I will respond to your report within 48 hours with my evaluation and expected resolution time
- I will keep you informed of the progress towards resolving the problem
- Once the security issue is fixed, I will publish a security advisory on GitHub