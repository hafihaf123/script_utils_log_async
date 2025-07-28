from setuptools import setup

_ = setup(
    name="script_utils_log_async",
    version="0.2.8",
    description="Minimal shared utilities for logging and main wrapping",
    author="Michal Buchman",
    author_email="misobuchta@gmail.com",
    license="MIT",
    packages=["script_utils_log_async"],
    package_data={"script_utils_log_async": ["py.typed"]},
    zip_safe=False,
)
