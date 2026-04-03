"""Repository rule to fix Bazel 6.x wrapped_clang missing LC_UUID on macOS 15+.

Bazel 6.x compiles wrapped_clang/libtool_check_unique with -Wl,-no_uuid, but
macOS 15+ (Sequoia/Tahoe) dyld requires LC_UUID. This rule recompiles them from
Bazel's bundled source with -Wl,-random_uuid.

This workaround is unnecessary on Bazel 7+ where wrapped_clang was removed.
"""

def _fix_macos_cc_toolchain_impl(repository_ctx):
    os_name = repository_ctx.os.name.lower()
    if "mac" not in os_name:
        repository_ctx.file("BUILD", "# Not on macOS, nothing to fix.\n")
        return

    # Force @local_config_cc to be generated first by resolving a label in it.
    cc_dir = str(repository_ctx.path(Label("@local_config_cc//:BUILD")).dirname)

    # Check if wrapped_clang already has LC_UUID (idempotent).
    result = repository_ctx.execute(["otool", "-l", cc_dir + "/wrapped_clang"])
    if "LC_UUID" in result.stdout:
        repository_ctx.file("BUILD", "# wrapped_clang already has LC_UUID.\n")
        return

    # Find the wrapped_clang.cc source in Bazel's install directory.
    result = repository_ctx.execute([
        "find", "/private/var/tmp",
        "-path", "*/embedded_tools/tools/osx/crosstool/wrapped_clang.cc",
        "-maxdepth", 8,
    ], timeout = 10)
    if result.return_code != 0 or not result.stdout.strip():
        fail("Could not find wrapped_clang.cc source in Bazel install.")

    wrapped_clang_src = result.stdout.strip().split("\n")[0]
    libtool_src = wrapped_clang_src.replace(
        "osx/crosstool/wrapped_clang.cc",
        "objc/libtool_check_unique.cc",
    )

    # Recompile wrapped_clang and wrapped_clang_pp with LC_UUID.
    for name in ["wrapped_clang", "wrapped_clang_pp"]:
        result = repository_ctx.execute([
            "clang++", "-std=c++11", "-O2", "-Wl,-random_uuid",
            "-o", cc_dir + "/" + name, wrapped_clang_src,
        ], timeout = 30)
        if result.return_code != 0:
            fail("Failed to recompile %s: %s" % (name, result.stderr))

    # Recompile libtool_check_unique with LC_UUID.
    result = repository_ctx.execute([
        "clang++", "-std=c++11", "-O2", "-Wl,-random_uuid",
        "-o", cc_dir + "/libtool_check_unique", libtool_src,
    ], timeout = 30)
    if result.return_code != 0:
        fail("Failed to recompile libtool_check_unique: %s" % result.stderr)

    repository_ctx.file("BUILD", "# macOS CC toolchain LC_UUID fix applied.\n")

fix_macos_cc_toolchain = repository_rule(
    implementation = _fix_macos_cc_toolchain_impl,
    local = True,
    doc = "Fixes Bazel 6.x wrapped_clang LC_UUID issue on macOS 15+.",
)
