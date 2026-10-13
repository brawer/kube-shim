output "public_ipv4" {
  description = "The host's public IPv4 address. Point your DNS record (whatever provider you use -- this module doesn't manage DNS) at this before setting acme_directory to \"production\"."
  value       = upcloud_server.kube_shim.network_interface[0].ip_address
}

output "server_id" {
  description = "UpCloud UUID of the host, e.g. for `upctl server show` or building a kubernetes_provider config that also needs to reference it directly."
  value       = upcloud_server.kube_shim.id
}

output "api_tokens" {
  description = "The bearer token(s) minted for the authenticated API (config.toml's server.api_tokens) -- feed these into your kubernetes_provider's token argument or kubectl config. Index 0 is always the first-minted token; raising api_token_count only ever appends, so existing indices keep their values across a rotation."
  value       = [for t in random_password.api_token : t.result]
  sensitive   = true
}

output "worker_ssh_public_key" {
  description = "The public half of the keypair this shim instance uses to SSH into the worker VMs it provisions. Not sensitive on its own, but exposed mainly for operator reference (e.g. to add to your own known_hosts tooling); the private half never leaves this module's state."
  value       = trimspace(tls_private_key.worker_ssh.public_key_openssh)
}
