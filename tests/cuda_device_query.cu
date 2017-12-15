#include <stdio.h>

int main()
{
	int deviceCount;
	int device;
	printf("Looking for CUDA devices...\n");
	cudaGetDeviceCount(&deviceCount);
	if (deviceCount == 0) {
		printf("No devices found\n");
	}
	for(device = 0; device < deviceCount; ++device) {
		cudaDeviceProp deviceProp;
		cudaGetDeviceProperties(&deviceProp, device);
		printf("device %d has compute capability %d.%d.\n",
			device, deviceProp.major, deviceProp.minor);
	}
}