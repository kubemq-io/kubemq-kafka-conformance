# Prebuilt image of a Go client driver (franz-go, sarama, kafka-go): the statically linked binary,
# cross-compiled for the target architecture by publish-drivers.sh, on the pinned static runtime
# image. Locally the track mounts the same binary into the same image instead.
ARG RUNTIME_IMAGE
FROM ${RUNTIME_IMAGE}
COPY driver /kcert/driver
ENTRYPOINT ["/kcert/driver"]
