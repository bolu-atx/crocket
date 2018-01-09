## Building tensorflow on CentOS 7

```
git clone https://github.com/tensorflow/tensorflow.git
cd tensorflow
git checkout r1.4

sudo touch /usr/include/stropts.h
bazel build -c opt --config=mkl --copt=-mavx --copt=-mfpmath=both --copt=-msse4.1 --copt=-msse4.2 //tensorflow/tools/pip_package:build_pip_package
bazel-bin/tensorflow/tools/pip_package/build_pip_package /tmp/tensorflow_pkg
```

 * Making /usr/include/stropts.h is a hacky fix (doesn't affect build).
