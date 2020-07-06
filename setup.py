from setuptools import setup

setup(
    name='skyportal-spatial', version='0.1.0', py_modules=['spatial'],
    install_requires=['sqlalchemy>=1.2.8',
                      'pytest>=3.3.0',
                      'numpy>=1.12.0',
                      'astropy>=4.0.0']
)
