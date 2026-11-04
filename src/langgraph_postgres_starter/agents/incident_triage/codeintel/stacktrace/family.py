"""The stack-trace families (spec §9.6 item 2).

The enum has its own module so that the parser modules can import it while the
package's ``__init__`` imports the parsers.
"""

from enum import StrEnum


class Family(StrEnum):
    """A family of runtimes that print stack traces in the same format.

    ``Frame.family`` holds the value, such as ``"jvm"``.
    """

    JVM = "jvm"  # Java, Kotlin and Scala
    NODE = "node"  # JavaScript and TypeScript on Node.js
    PYTHON = "python"
    GO = "go"
    PHP = "php"
    RUBY = "ruby"
    NATIVE = "native"  # C and C++
