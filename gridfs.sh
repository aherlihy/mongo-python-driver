for i in {0..100}; do dd if=/dev/urandom of="file"$i".txt" bs=1048576 count=5; done
