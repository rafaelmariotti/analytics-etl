import pytest
from scripts.s3.s3 import validate_s3_interface

class TestS3:
    def test_validate_s3_interface_with_s3a_raises_exception(self):
        save_s3 = True
        use_s3_dist_cp = True
        s3_bucket = "s3a://data-lake/stage"
        with pytest.raises(Exception):
            validate_s3_interface(save_s3, use_s3_dist_cp, s3_bucket)

    def test_validate_s3_interface_with_s3_raises_exception(self):
        save_s3 = True
        use_s3_dist_cp = False
        s3_bucket = "s3://data-lake/stage"
        with pytest.raises(Exception):
            validate_s3_interface(save_s3, use_s3_dist_cp, s3_bucket)
