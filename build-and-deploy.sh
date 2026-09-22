#!/bin/sh
set -eu

# Manual, local escape-hatch deploy: builds the same fast-jar tarball that
# _release.yml produces in CI, then hands it to bin932-home's own
# deploy-web-service.sh over SSH - the same script both the GitHub-Release
# based CI deploy and a developer's `just deploy sand`/`deploy prod` use
# (see bin932-home's ansible/roles/web-service/templates/deploy-web-service.sh.j2
# and tasks/bootstrap-or-update.yml), just fed a locally-built tarball
# instead of a downloaded release asset. Never touches GitHub Releases
# itself. Release names always get a "draft-" prefix so they can never be
# confused with a permanent, CI-published "build-<version>" release.

# Defaults for ready-bell-demo, from bin932-home's
# ansible/inventory/services.yml and ansible/roles/web-service/defaults/main.yml
# - override if the target host differs. deploy-web-service.sh derives the
# systemd unit (web-ready-bell-demo.service) and the release directory itself
# from SERVICE_NAME, so this is the only name this script needs.
SERVICE_NAME="ready-bell-demo"
DEPLOY_SCRIPT_PATH="/usr/local/bin/deploy-web-service.sh"

usage() {
  # host is normally cicd@<host> - that's the account bin932-home grants a
  # narrow, passwordless sudo rule for running deploy-web-service.sh (see
  # its tasks/main.yml, "Allow cicd to run deploy-web-service.sh").
  echo "Usage: $0 <user@host>" >&2
  exit 1
}

[ $# -eq 1 ] || usage
host="$1"

version="$(git describe --tags --match="v[0-9]*.*" --long --dirty)"
release_name="draft-$version"
echo "==> Release name: $release_name"

echo "==> Building fast-jar"
mvn -B --color=always package -DskipTests

echo "==> Assembling release payload"
repo_root="$(pwd)"
mkdir -p target/release-payload
cat > target/release-payload/run.sh <<'EOF'
#!/bin/sh
script_dir="$(cd "$(dirname "$0")" && pwd)"
exec java -jar "$script_dir/quarkus-app/quarkus-run.jar"
EOF
chmod +x target/release-payload/run.sh
# Tar straight from target/quarkus-app rather than cp -r'ing it into
# release-payload/ first - that tree is mostly jars and can be large, so
# skipping the copy avoids a redundant full read+write pass. gzip -1
# instead of tar's default -z (level 6) for the same reason: the payload
# is mostly already-DEFLATE-compressed jars, so a higher level just burns
# CPU for negligible extra shrinkage. --no-xattrs drops macOS's synthetic
# com.apple.provenance xattr, which otherwise rides along as a pax extended
# header and makes the remote (GNU tar) extraction print "Ignoring unknown
# extended header keyword" for every file.
tar --no-xattrs -cf - -C "$repo_root/target" quarkus-app -C "$repo_root/target/release-payload" run.sh \
  | gzip -1 > target/release.tar.gz

remote_tarball="/tmp/$SERVICE_NAME-$release_name.tar.gz"

echo "==> Copying release.tar.gz to $host:$remote_tarball"
scp target/release.tar.gz "$host:$remote_tarball"

echo "==> Running deploy-web-service.sh on $host"
ssh "$host" "sudo '$DEPLOY_SCRIPT_PATH' '$SERVICE_NAME' '$remote_tarball' '$release_name' && rm -f '$remote_tarball'"

echo "==> Deployed $release_name to $host"
