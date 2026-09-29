# Droid

T3 Code can use your Factory Droid CLI with your own Factory account, from the web, desktop, or
mobile app. Droid support is in Early Access.

## Set Up Droid

1. Install the [Droid CLI](https://docs.factory.ai/cli/getting-started/quickstart) on the machine
   running the T3 Code server.
2. Run `droid` once in a terminal and finish the browser sign-in.
3. Open T3 Code Settings, enable Droid, and refresh the provider.

If `droid` is not on the server's `PATH`, set Droid's binary path to the executable. To use an API
key instead of the signed-in account, add `FACTORY_API_KEY` to the Droid provider's environment
variables and mark it as sensitive.

T3 Code cannot verify a Factory login without starting a session, so the provider card shows the
sign-in as unverified. If a turn reports that Droid is not signed in, run `droid` in a terminal to
sign in again.

## Models And Reasoning

T3 Code lists the models Droid reports, with the reasoning efforts each model supports. **Droid
default** keeps the model chosen in your Factory settings. You can change the model or reasoning
effort in an existing thread; the change applies to the next message.

Droid slash commands and skills appear in the composer after a provider refresh.

## Permission Modes

| T3 Code mode      | Droid autonomy                                 |
| ----------------- | ---------------------------------------------- |
| Supervised        | Asks before every command and file change      |
| Auto-accept edits | Allows edits and read-only commands            |
| Auto              | Also allows reversible commands without asking |
| Full access       | Allows all commands without asking             |

Declining an approval ends Droid's current response. Send another message to tell Droid how to
proceed.

## Plan Mode

Plan mode uses Droid's Spec Mode. When Droid proposes its spec, T3 Code shows it as a proposed plan
and Droid stops without implementing. Implement the plan from T3 Code, or reply in plan mode to
refine it.

## Threads

Messages sent while Droid is working steer the current response. Threads resume across server
restarts, and checkpoint rollback rewinds the Droid conversation along with your files. Forking a
Droid thread carries the conversation over as a summary. Subagents Droid starts appear as tasks in
the conversation.
