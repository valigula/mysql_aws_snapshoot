import unittest
import slackmanager as sm
import offline_backup as backup


class TestStringMethods(unittest.TestCase):

    def test_upper(self):
        self.assertEqual('foo'.upper(), 'FOO')

    def test_isupper(self):
        self.assertTrue('FOO'.isupper())
        self.assertFalse('Foo'.isupper())

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)


class TestOfflineBackup(unittest.TestCase):

    def test_createSnapshot(self):
        sm.SlackManager.send_message("lalalala")

    def test_offlinebackup(self):
        backup.get_lock_file()


if __name__ == '__main__':
    unittest.main()