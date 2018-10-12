import setuptools

module = setuptools.Extension(
        "ctraildb",
        sources=['traildbmodule.c'],
        libraries=['traildb'])

setuptools.setup(
        name='ctraildb',
        version='0.1',
        author='Mikko Juola',
        author_email='mikko.juola@adroll.com',
        description='TrailDB Python bindings',
        packages=setuptools.find_packages(),
        ext_modules=[module]
)
