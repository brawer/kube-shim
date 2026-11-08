terraform {
  required_version = ">= 1.6.0"

  required_providers {
    # 5.17.0 added template.tier on upcloud_server.
    upcloud = {
      source  = "UpCloudLtd/upcloud"
      version = ">= 5.17.0"
    }
    tls = {
      source  = "hashicorp/tls"
      version = ">= 4.0.0"
    }
    random = {
      source  = "hashicorp/random"
      version = ">= 3.6.0"
    }
  }
}
