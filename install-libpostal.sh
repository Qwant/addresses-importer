LIBPOSTAL_COMMIT="95f31de3b25eaf0b23c8efd97b1243d9d690ba58"

git clone https://github.com/openvenues/libpostal libpostal
cd libpostal
git checkout ${LIBPOSTAL_COMMIT}

./bootstrap.sh
./configure --datadir=/srv/data
make install -j 4

cd ..
rm -rf libpostal
