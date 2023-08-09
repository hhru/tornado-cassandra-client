from functools import partial
import os.path
import unittest

import pycodestyle

from tests import PROJECT_ROOT


class TestPycodestyle(unittest.TestCase):
    CHECKED_PATHS = ('tassandra', 'tests')

    def test_pycodestyle(self):
        style_guide = pycodestyle.StyleGuide(
            show_pep8=False,
            show_source=True,
            max_line_length=120
        )
        result = style_guide.check_files(map(partial(os.path.join, PROJECT_ROOT), TestPycodestyle.CHECKED_PATHS))
        assert result.total_errors == 0, 'Pep8 found code style errors or warnings'
