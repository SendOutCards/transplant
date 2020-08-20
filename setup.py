from setuptools import setup, find_packages

setup(
    name='soc-transplant',
    version='0.2.0',
    description='Framework for seeding databases from another',
    author='Tyler Lovely',
    author_email='tyler.n.lovely@gmail.com',
    packages=find_packages(),
    install_requires=open('requirements.txt').readlines(),
    python_requires='>=3.6'
)
