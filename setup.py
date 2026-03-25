from setuptools import setup, find_packages

setup(
    name="onboarding-utils",
    version="0.1.0",
    packages=find_packages(),
    py_modules=['cli'], 
    include_package_data=True,
    install_requires=[
        "pandas==2.3.2",
        "PyYAML==6.0.2",
        "pyfiglet",
        "openpyxl"
    ],
    python_requires=">=3.8",
    author="DB Engineering",
    description="A collection of tools for device onboarding.",
    entry_points={
        'console_scripts': [
            'onboard=cli:main',
        ],
    },
)