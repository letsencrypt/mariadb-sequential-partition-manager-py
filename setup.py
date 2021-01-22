from setuptools import setup


setup(
    name="mariadb-autoincrement-partition-manager",
    version="0.0.1",
    description="Manage DB partitions based on autoincrement IDs",
    long_description="Manage MariaDB Partitions based on Autoincrement IDs",
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3",
    ],
    keywords="database",
    url="http://github.com/jcjones/mariadb-autoincrement-partition-manager",
    author="J.C. Jones",
    author_email="jc@insufficient.coffee",
    license="Mozilla Public License 2.0 (MPL 2.0)",
    zip_safe=False,
    include_package_data=True,
    python_requires=">=3.6",
    install_requires=[],
    packages=["partitionmanager"],
    entry_points={
        "console_scripts": ["autoincrement-partition-manager=partitionmanager.cli:main"]
    },
)
