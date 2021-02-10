#!/usr/bin/env bash
set -x

# Install kvm
sudo apt-get install -y kvm qemu-kvm libvirt-bin virtinst
# Start libvirtd
sudo systemctl start libvirtd
# Setup nested virtualization
sudo modprobe -r kvm_intel
sudo modprobe kvm_intel nested=1
