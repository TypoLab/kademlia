from setuptools import setup

version = '0.0.1'

setup(
    name='kademlia',
    packages=['kademlia'],
    version=version,
    description='A Kademlia DHT protocol asyncio implementation',
    author='Chen Shuaimin',
    author_email='chen_shuaimin@outlook.com',
    url='https://github.com/TypoLab/kademlia',
    python_requires='>=3.7',
    install_requires=['aiohttp', 'argparse', 'msgpack'],
    entry_points={
        'console_scripts': [
            'kad = kademlia.demo:main'
        ]
    }
)
