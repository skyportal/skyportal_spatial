from setuptools import setup

setup(
    name='skyportal_spatial', version='0.1.0',
    install_requires=['sqlalchemy>=1.2.8',
                      'pytest>=3.3.0',
                      'numpy>=1.12.0',
                      'astropy>=4.0.0',
                      'pyyaml>=5.0.0',
                      'psycopg2>=2.5.3',
                      'scipy>=1.0.0'],
    packages=['skyportal_spatial']
)
