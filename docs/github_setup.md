# GitHub setup

1. Create a repository, e.g. `coindcx-delta-research`.
2. Upload the repository contents **including the folder hierarchy**.
3. Do not upload `.venv` or collected data files.
4. On GitHub, open the repository's Actions tab and confirm `Python checks` passes.
5. Clone the repository on the machine that will actually collect the data; GitHub itself is source control, not the runtime. The collector needs a machine/runner with outbound internet access and a persistent process.

Example:

```bash
git clone <your-repo-url>
cd coindcx-delta-research
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python scripts/run_collector.py --config config/config.json --minutes 60
```

For long collection, use a VPS/server, Docker, systemd, screen/tmux, or another persistent runner. Commit only code/config/docs; keep raw datasets outside Git when they become large.
