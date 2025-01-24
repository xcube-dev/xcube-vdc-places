from distutils.core import setup

requirements = [
    # Use ./environment.yml for deps.
]

setup(
    name='xcube-vdc-places',
    version='0.1.dev0',
    packages=['xcube_vdc_plugin', 'xcube_vdc_plugin.api',
              'xcube_vdc_plugin.server'],
    url='https://github.com/xcube-dev/xcube-vdc-places',
    license='MIT License',
    author='Tonio Fincke',
    description=
    'A plugin for xcube server that reads vector data cubes as feature data.',
    install_requires=requirements
)
