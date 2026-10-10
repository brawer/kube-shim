# Changelog

## [0.2.4](https://github.com/brawer/kube-shim/compare/v0.2.3...v0.2.4) (2026-10-10)


### 🆕 Enhancements

* allow negative budget top-ups to correct over-generous ones ([0e42019](https://github.com/brawer/kube-shim/commit/0e420197b72a3797204567a88be110475d7352fb))
* Phase 14b -- rolling budget guard, top-up, and mutable settings ([4b762a2](https://github.com/brawer/kube-shim/commit/4b762a2abde09ff4faf1e90501b0612455f82bb7))


### 🐞 Bug Fixes

* record entering BudgetWait as a Warning event, not Normal ([b4c8f99](https://github.com/brawer/kube-shim/commit/b4c8f9999637842cf75cc7cfe574b12753b3ef5f))

## [0.2.3](https://github.com/brawer/kube-shim/compare/v0.2.2...v0.2.3) (2026-10-08)


### 🆕 Enhancements

* Phase 14a -- pricing, cost tracking, and a real FOCUS cost report ([#61](https://github.com/brawer/kube-shim/issues/61)) ([745755b](https://github.com/brawer/kube-shim/commit/745755bf7a2ce6f238833ec9b7afb3c672b64569))


### 🐞 Bug Fixes

* use kube-shim.brawer.ch, not kube-shim.io, in the admission webhook name ([#58](https://github.com/brawer/kube-shim/issues/58)) ([4a6419a](https://github.com/brawer/kube-shim/commit/4a6419a85e2ea6ea97752f18a3c6cb1a2ad4fbba))


### 📚 Documentation

* don't hardcode EUR as UpCloud's billing currency, read it from the API ([#60](https://github.com/brawer/kube-shim/issues/60)) ([70c46e2](https://github.com/brawer/kube-shim/commit/70c46e20806d59d3a18a74c81836b18c11195a98))
* record live verification of v0.2.2 against kube-shim.brawer.ch ([#56](https://github.com/brawer/kube-shim/issues/56)) ([37a06ca](https://github.com/brawer/kube-shim/commit/37a06cadf204af64371810ce47b50b33c1f1f79f))
* split Phase 14 into cost tracking (14a) and budget enforcement (14b) ([#59](https://github.com/brawer/kube-shim/issues/59)) ([c3a0bb3](https://github.com/brawer/kube-shim/commit/c3a0bb322b99f8e911a081d721a3c9cf15910e66))

## [0.2.2](https://github.com/brawer/kube-shim/compare/v0.2.1...v0.2.2) (2026-10-05)


### 🆕 Enhancements

* standalone batch/v1 Jobs, not only CronJob-spawned runs ([#55](https://github.com/brawer/kube-shim/issues/55)) ([4f6e941](https://github.com/brawer/kube-shim/commit/4f6e941c4c364d7112ad73f7009a4acc6a9c60f3))


### 📚 Documentation

* plan Phase 13 -- standalone batch/v1 Jobs ([#53](https://github.com/brawer/kube-shim/issues/53)) ([aefda0f](https://github.com/brawer/kube-shim/commit/aefda0f08a826deaba62c25e6d74f32919a392e7))

## [0.2.1](https://github.com/brawer/kube-shim/compare/v0.2.0...v0.2.1) (2026-10-05)


### 🆕 Enhancements

* real worker-VM metrics over SSH, replacing the request-based estimate ([#50](https://github.com/brawer/kube-shim/issues/50)) ([adc63f4](https://github.com/brawer/kube-shim/commit/adc63f43f2b1551d3d568985f69105375ec70a04))


### 📚 Documentation

* plan Phase 17 -- Prometheus /metrics (self-instrumentation + worker-VM VM stats) ([#51](https://github.com/brawer/kube-shim/issues/51)) ([e4330cc](https://github.com/brawer/kube-shim/commit/e4330cc43f3d0b0fdc5018b79f1556cfdc0a974f))
* plan Phase 17 -- Prometheus /metrics (shim self-instrumentation + worker-VM virtual-memory stats) ([e4330cc](https://github.com/brawer/kube-shim/commit/e4330cc43f3d0b0fdc5018b79f1556cfdc0a974f))

## [0.2.0](https://github.com/brawer/kube-shim/compare/v0.1.13...v0.2.0) (2026-10-04)


### 🆕 Enhancements

* Events + Metrics APIs (Phase 12) ([#46](https://github.com/brawer/kube-shim/issues/46)) ([19560f9](https://github.com/brawer/kube-shim/commit/19560f933cdac877799363c90a89a0bfa481e06f))
* honor resources.limits on the worker VM, to de-risk migration to real k8s ([#48](https://github.com/brawer/kube-shim/issues/48)) ([b30381c](https://github.com/brawer/kube-shim/commit/b30381c36c6c29565dd893fd19502f53d23f692c))


### 🚧 Maintenance

* release as 0.2.0 ([#49](https://github.com/brawer/kube-shim/issues/49)) ([776f5d6](https://github.com/brawer/kube-shim/commit/776f5d690fbeff7da5d2edbc0953a492f1fd99e0))

## [0.1.13](https://github.com/brawer/kube-shim/compare/v0.1.12...v0.1.13) (2026-10-04)


### 🐞 Bug Fixes

* stop worker VMs before deleting them (UpCloud rejects delete on a running server) ([#45](https://github.com/brawer/kube-shim/issues/45)) ([c5edf38](https://github.com/brawer/kube-shim/commit/c5edf38f6db25c9fb4c52920d274bc66a5cfb8b7))


### 📚 Documentation

* update README past Phase 1, point to the implementation plan ([#43](https://github.com/brawer/kube-shim/issues/43)) ([3583e9f](https://github.com/brawer/kube-shim/commit/3583e9f737dce66abc8bf54a426221b0b5a3aba6))

## [0.1.12](https://github.com/brawer/kube-shim/compare/v0.1.11...v0.1.12) (2026-10-02)


### 🆕 Enhancements

* idempotent retries, deadline/timeout enforcement, network timeouts (Phase 11) ([#41](https://github.com/brawer/kube-shim/issues/41)) ([5263836](https://github.com/brawer/kube-shim/commit/526383675c64db24aefb5ce9ad1bca912d0038c0))

## [0.1.11](https://github.com/brawer/kube-shim/compare/v0.1.10...v0.1.11) (2026-09-29)


### 🆕 Enhancements

* real SSH-based log streaming and exit-code capture (Phase 10) ([#39](https://github.com/brawer/kube-shim/issues/39)) ([42b9b22](https://github.com/brawer/kube-shim/commit/42b9b221d9cbcd851ca9528006661587a172d6e7))

## [0.1.10](https://github.com/brawer/kube-shim/compare/v0.1.9...v0.1.10) (2026-09-29)


### 🐞 Bug Fixes

* Containerfile missing bootstrap/cloud-init-template.sh ([#37](https://github.com/brawer/kube-shim/issues/37)) ([2ad092b](https://github.com/brawer/kube-shim/commit/2ad092b63ab54b15021198af775a12e6fddc8681))
* Containerfile release build missing bootstrap/cloud-init-template.sh ([2ad092b](https://github.com/brawer/kube-shim/commit/2ad092b63ab54b15021198af775a12e6fddc8681))

## [0.1.9](https://github.com/brawer/kube-shim/compare/v0.1.8...v0.1.9) (2026-09-27)


### 🆕 Enhancements

* real VM provisioning, cloud-init, firewall isolation (Phase 9) ([#36](https://github.com/brawer/kube-shim/issues/36)) ([4c4ce22](https://github.com/brawer/kube-shim/commit/4c4ce2208d058c2010535c53f2abd25c91e035db))


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
