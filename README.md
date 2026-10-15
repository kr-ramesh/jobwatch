# jobwatch

A desktop app that watches your jobs on remote servers and notifies you when they finish.

- Lists the servers in your `~/.ssh/config`. You tick the ones to watch.
- On Slurm clusters it shows your running, pending and recently finished jobs, with their state and exit code. On servers without Slurm it tracks your processes that match a pattern (default: `python`, `torchrun`, …).
- **Watches:** select jobs (or give a name pattern like `sweep-*`, or "all jobs on this server") and get one notification when the whole set is done, with how many completed or failed.
- Optionally sends a notification for every job that finishes.
- Notifications go to your desktop, and optionally to your phone through [ntfy](https://ntfy.sh).
- Keeps checking in the background after you close the window. It can also start at login.
- Jobs that end while jobwatch is closed are reported the next time it runs.

## Install

**Linux Mint / Ubuntu / Debian:** download `jobwatch_<version>_all.deb` from the [latest release](https://github.com/kr-ramesh/jobwatch/releases/latest) and double-click it, or run:

```
sudo apt install ./jobwatch_0.1.0_all.deb
```

Then open **jobwatch** from the app menu.

**Other Linux, or without sudo:** `./install.sh` installs it for your user only. `./install.sh --uninstall` removes it.

**macOS:** `python3 jobwatch.py` opens the dashboard in your browser. Notifications use Notification Center.

## Server requirements

jobwatch runs `ssh <host>` without a terminal, so each server must log in **without a password or 2FA prompt**:

- Key-based login works out of the box. On Mint, the login keyring unlocks your SSH keys.
- For servers with 2FA, turn on connection sharing in `~/.ssh/config`. Then log in once in a terminal, and jobwatch reuses that connection while it stays open:

  ```
  Host mycluster
      ControlMaster auto
      ControlPath ~/.ssh/cm-%C
      ControlPersist 12h
  ```

Nothing is installed on the servers. Each check runs `squeue` and `sacct` (or `ps`) through ssh.

## Command line

```
jobwatch                    open the window (starts background checking if needed)
jobwatch --hosts a b        also watch servers a and b
jobwatch --browser          open in a browser tab instead of an app window
jobwatch --quit             stop background checking
jobwatch --service          run the checker in the foreground (logs to the terminal)
```

Settings, watches and history are stored in `~/.config/jobwatch/`. The checker's log is `~/.config/jobwatch/service.log`.

## Privacy

The dashboard is served only on `127.0.0.1`. Job data stays on your machine, unless you set an ntfy topic: then notification text (job names and states) is sent to that ntfy server.

## Building the .deb

```
./build-deb.sh        # writes dist/jobwatch_<version>_all.deb
```

Bump `VERSION` at the top of `build-deb.sh` for each release.

To update an installed copy: install the new `.deb` over the old one, then run `jobwatch --quit` and reopen jobwatch so the background checker runs the new version.

## License

GPL-3.0. See [LICENSE](LICENSE).
