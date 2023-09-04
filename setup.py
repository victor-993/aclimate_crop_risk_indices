from setuptools import setup, find_packages

setup(
    name="aclimate_crop_risk_indices",
    version='v0.0.0',
    description="Resampling module",
    url="https://github.com/victor-993/aclimate_crop_risk_indices",
    download_url="https://github.com/victor-993/aclimate_crop_risk_indices",
    packages=find_packages('src'),
    package_dir={'': 'src'},
    keywords='aclimate crop risk indices',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    entry_points={
        'console_scripts': [
            'aclimate_crop_risk=aclimate_crop_risk_indices.aclimate_run_crop_risk:main',
        ],
    },
    install_requires=[
        "pandas==2.0.3",
        "scipy==1.11.2",
        "tqdm==4.66.1",
    ]
)