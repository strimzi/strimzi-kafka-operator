#!/usr/bin/env bash
set -x

# Install kvm
sudo apt-get install -y qemu-kvm libvirt-bin virtinst bridge-utils cpu-checker
# Add user to libvirt groups
sudo usermod -aG kvm $USER
sudo usermod -aG libvirt $USER
# Start libvirtd
sudo systemctl start libvirtd
kvm-ok
# Setup nested virtualization
sudo modprobe -r kvm_intel
sudo modprobe kvm_intel nested=1
