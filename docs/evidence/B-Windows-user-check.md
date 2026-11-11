# Student-reported Windows verification

Source: PowerShell output and browser screenshots supplied by the student in this conversation on 10 November 2026. These checks were performed on the student's machine, not by this agent on Windows.

- Runtime: Windows, official CPython 3.13.3, invoked through `.venv-win/Scripts/python.exe`.
- Original result: 1 failed, 50 passed, 2 skipped. Failure: test_login_broadcast_private_end_to_end waited for an arbitrary event queue length; login/user events could satisfy that condition before chat arrival.
- Fix: wait for exact broadcast/private messages with sender/text/private fields and a bounded timeout; introduce simulated delivery delay; clean up in finally.
- Local regression: delayed chat case passed 10 consecutive runs; Linux suite 51 passed, 2 skipped in 21.53 s.
- Student rerun: `51 passed, 2 skipped in 22.13s`.
- Browser checks: Alice/Bob login and online list, private/global chat, accepted file with successful hash result at both ends, rejected file notification.
- Server restart: student stopped and restarted server while clients/pages stayed open, then confirmed receipt of a new message without logging in again.

Still not verified: integrated TCP bootstrap, bounded async relay, real transport statistics, large-file RAM bounds, physical two-machine LAN and final submission report. The bundled PDF is an earlier integration draft; this file supplements its Windows/browser evaluation status.
