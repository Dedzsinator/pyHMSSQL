"""Legacy Haskell Parser module for pyHMSSQL.

This module is now deprecated in favor of SQLGlot parsing.
Kept for backward compatibility if needed.
"""

import logging
from typing import Dict, Any, Optional


class HaskellParser:
    """
    Legacy Haskell SQL parser - now deprecated.

    SQLGlot is now the primary parser. This class is kept for
    backward compatibility but should not be used for new code.
    """

    def __init__(self, binary_path: Optional[str] = None):
        """Initialize the deprecated Haskell parser."""
        logging.warning("⚠️  HaskellParser is deprecated. Use SQLGlotParser instead.")
        self.binary_path = binary_path

    def parse(self, sql: str) -> Dict[str, Any]:
        """Parse method - now deprecated."""
        logging.warning("⚠️  HaskellParser.parse() is deprecated. Use SQLGlotParser instead.")
        return {"error": "HaskellParser is deprecated. Use SQLGlotParser instead."}
