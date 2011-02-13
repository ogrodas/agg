import agg
import unittest
import os
import filecmp


class TestAggInt(unittest.TestCase):
    def setUp(self):
        for file in os.listdir('tests'):
            if file.endswith("test"):
                os.remove(os.path.join("tests",file))
        
    def test_onlycount(self):
        """equal to wc l"""
        agg.main("--count= --file=tests/count1.test tests/testdata".split())
        self.assertTrue( filecmp.cmp(  "tests/count1.expected","tests/count1.test" ) )
    
    def test_count(self):
        agg.main("--groupby=4 --count= --file=tests/count2.test tests/testdata".split())
        self.assertTrue( filecmp.cmp(  "tests/count2.expected","tests/count2.test" ) )
    
    def test_uniqcount(self):
        agg.main("--groupby=4 --countuniq=5 --file=tests/countuniq1.test tests/testdata".split())
        self.assertTrue( filecmp.cmp(  "tests/countuniq1.expected","tests/countuniq1.test" ) )
        
    def test_sum(self):
        agg.main("--groupby=4 --sum=5 --file=tests/sum1.test tests/testdata".split())
        self.assertTrue( filecmp.cmp(  "tests/sum1.expected","tests/sum1.test" ) )

    def test_concat(self):
        agg.main("--groupby=4 --concat=5 --file=tests/concat1.test tests/testdata".split())
        self.assertTrue( filecmp.cmp(  "tests/concat1.expected","tests/concat1.test" ) )

    def test_uniqconcat(self):
        agg.main("--groupby=4 --concatuniq=5 --file=tests/concatuniq1.test tests/testdata".split())
        self.assertTrue( filecmp.cmp(  "tests/concatuniq1.expected","tests/concatuniq1.test" ) )

    def test_max(self):
        agg.main("--groupby=4 --max=5 --file=tests/max1.test tests/testdata".split())
        self.assertTrue( filecmp.cmp(  "tests/max1.expected","tests/max1.test" ) )

    def test_min(self):
        agg.main("--groupby=4 --min=5 --file=tests/min1.test tests/testdata".split())
        self.assertTrue( filecmp.cmp(  "tests/min1.expected","tests/min1.test" ) )

if __name__ == '__main__':
    unittest.main()




