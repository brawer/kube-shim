# Changelog

## [0.1.9](https://github.com/brawer/kube-shim/compare/v0.1.8...v0.1.9) (2026-09-25)


### 🐞 Bug Fixes

* cd to / before the podman-unshare chown step in provision.sh ([ba49500](https://github.com/brawer/kube-shim/commit/ba49500f998b7e8b7769b2b52a842b0a11a281e1))
* provision.sh chdir failure when run from /root ([#34](https://github.com/brawer/kube-shim/issues/34)) ([ba49500](https://github.com/brawer/kube-shim/commit/ba49500f998b7e8b7769b2b52a842b0a11a281e1))

## [0.1.8](https://github.com/brawer/kube-shim/compare/v0.1.7...v0.1.8) (2026-09-20)


### 🆕 Enhancements

* real UpCloud volume operations, orphan scanning (Phase 8) ([#32](https://github.com/brawer/kube-shim/issues/32)) ([137d447](https://github.com/brawer/kube-shim/commit/137d447c62c75b13bfdac8e71c5ff28a0708ca7e))

## [0.1.7](https://github.com/brawer/kube-shim/compare/v0.1.6...v0.1.7) (2026-09-18)


### 🆕 Enhancements

* UpCloud CloudProvider integration, dry-run mode (Phase 7) ([#30](https://github.com/brawer/kube-shim/issues/30)) ([926b4b4](https://github.com/brawer/kube-shim/commit/926b4b405738fda63764516c800faf0c94870a3a))

## [0.1.6](https://github.com/brawer/kube-shim/compare/v0.1.5...v0.1.6) (2026-09-18)


### 🆕 Enhancements

* reconciliation loop skeleton (Phase 6) ([#28](https://github.com/brawer/kube-shim/issues/28)) ([7311c2f](https://github.com/brawer/kube-shim/commit/7311c2fc9d9281d5acd92ef3f233f69b8dabaf44))

## [0.1.5](https://github.com/brawer/kube-shim/compare/v0.1.4...v0.1.5) (2026-09-18)


### 🆕 Enhancements

* generic ephemeral volumes + CronJob admission checks (Phase 5) ([#26](https://github.com/brawer/kube-shim/issues/26)) ([cdf7ca7](https://github.com/brawer/kube-shim/commit/cdf7ca71ed1146bb4197120b698468512a6fc662))

## [0.1.4](https://github.com/brawer/kube-shim/compare/v0.1.3...v0.1.4) (2026-09-18)


### 🐞 Bug Fixes

* ACME challenge-responder deadlock + sysctl value ([#24](https://github.com/brawer/kube-shim/issues/24)) ([0877242](https://github.com/brawer/kube-shim/commit/0877242ba24e2005639c377dc20ce310390c1146))

## [0.1.3](https://github.com/brawer/kube-shim/compare/v0.1.2...v0.1.3) (2026-09-18)


### 🆕 Enhancements

* automatic TLS via ACME (Phase 4) ([#23](https://github.com/brawer/kube-shim/issues/23)) ([f334c4a](https://github.com/brawer/kube-shim/commit/f334c4ab6561e3b9d570fd710262920b4913edd2))


### 🐞 Bug Fixes

* gitignore the local secrets/ directory ([#17](https://github.com/brawer/kube-shim/issues/17)) ([e6ce0f2](https://github.com/brawer/kube-shim/commit/e6ce0f2737cfe8131795ec4836e67944296c7b81))


### 📚 Documentation

* fold ACME/UpCloud/ephemeral-volumes design round into the plan ([#20](https://github.com/brawer/kube-shim/issues/20)) ([1302dd3](https://github.com/brawer/kube-shim/commit/1302dd336171f3e51e904435793028a78a7c72b6))
* fold the ACME/UpCloud/ephemeral-volumes design round into the plan ([1302dd3](https://github.com/brawer/kube-shim/commit/1302dd336171f3e51e904435793028a78a7c72b6))
* move public status page onto :443, add z-pages, defer /metrics ([#21](https://github.com/brawer/kube-shim/issues/21)) ([16071fd](https://github.com/brawer/kube-shim/commit/16071fddd1e15ec7f6cab1a129e2265d39832bec))
* record verified rootless-podman privileged-port finding (Phase 4) ([#22](https://github.com/brawer/kube-shim/issues/22)) ([ebb25e0](https://github.com/brawer/kube-shim/commit/ebb25e0de6c83a46d5e019792fdfaed4a4572792))
* switch worker containers to podman, enable auto-update for kube-shim.brawer.ch ([#18](https://github.com/brawer/kube-shim/issues/18)) ([92293a3](https://github.com/brawer/kube-shim/commit/92293a3e5d97e51deca2fb8a5f121267f2cd2c95))

## [0.1.2](https://github.com/brawer/kube-shim/compare/v0.1.1...v0.1.2) (2026-09-16)


### 🆕 Enhancements

* multi-arch podman-based release build with attest provenance ([#15](https://github.com/brawer/kube-shim/issues/15)) ([802f9ee](https://github.com/brawer/kube-shim/commit/802f9ee5043c9ec571acb09725530d880a6bc678))


### 🐞 Bug Fixes

* commit Cargo.lock so release builds actually work ([#13](https://github.com/brawer/kube-shim/issues/13)) ([29cba19](https://github.com/brawer/kube-shim/commit/29cba1975eb58adedafb792ec944994e5588f31d))

## [0.1.1](https://github.com/brawer/kube-shim/compare/v0.1.0...v0.1.1) (2026-09-16)


### 🆕 Enhancements

* add release-please for automated versioning and releases ([#11](https://github.com/brawer/kube-shim/issues/11)) ([f8e659c](https://github.com/brawer/kube-shim/commit/f8e659cf37bd321e9059f6ceb1c6a9c99a014bd9))
