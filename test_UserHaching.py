from UserHashing import AssignUserToBucket

def test1():
    """Normal scenario
    """
    userId = "25HE93HQ635J131GDHWPQ98Y3215G12G"
    salt =   "ASHT83761GH104H1-1EB1HJL37312B35"
    assert AssignUserToBucket(userId, salt) == 455, "Should be 455"


def test2():
    """New salt
    """
    userId = "25HE93HQ635J131GDHWPQ98Y3215G12G"
    salt =   "438HGGHTL484P013HLKQ46HLO4Q6HLKV"
    assert AssignUserToBucket(userId, salt) == 422, "Should be 422"


def test3():
    """New user id
    """
    userId = "HDASLK563446HL78Z8G2G1LDQ84HTGAL"
    salt =   "ASHT83761GH104H1-1EB1HJL37312B35"
    assert AssignUserToBucket(userId, salt) == 961, "Should be 961"


def test4():
    """Empty userId
    """
    userId = ""
    salt =   "ASHT83761GH104H1-1EB1HJL37312B35"
    assert AssignUserToBucket(userId, salt) == -1, "Should be -1"


def test5():
    """Empty salt
    """
    userId = "HDASLK563446HL78Z8G2G1LDQ84HTGAL"
    salt =   ""
    assert AssignUserToBucket(userId, salt) == -1, "Should be -1"


def test6():
    """None userId
    """
    userId = None
    salt =   "ASHT83761GH104H1-1EB1HJL37312B35"
    assert AssignUserToBucket(userId, salt) == -1, "Should be -1"


def test7():
    """None salt
    """
    userId = "HDASLK563446HL78Z8G2G1LDQ84HTGAL"
    salt =   None
    assert AssignUserToBucket(userId, salt) == -1, "Should be -1"


def test8():
    """Different format strings
    """
    userId = "25HE93HQ635J"
    salt =   "ASHT83761GH104H"
    assert AssignUserToBucket(userId, salt) == 249, "Should be 249"


def test9():
    """Integer ids
    """
    userId = 1735
    salt =   65
    assert AssignUserToBucket(userId, salt) == 966, "Should be 966"


def test10():
    """Fewer buckets
    """
    userId = "25HE93HQ635J131GDHWPQ98Y3215G12G"
    salt =   "ASHT83761GH104H1-1EB1HJL37312B35"
    assert AssignUserToBucket(userId, salt, 100) == 55, "Should be 55"

def test11():
    """10 buckets
    """
    userId = "25HE93HQ635J131GDHWPQ98Y3215G12G"
    salt =   "ASHT83761GH104H1-1EB1HJL37312B35"
    assert AssignUserToBucket(userId, salt, 10) == 5, "Should be 5"

def test12():
    """1 bucket
    """
    userId = "25HE93HQ635J131GDHWPQ98Y3215G12G"
    salt =   "ASHT83761GH104H1-1EB1HJL37312B35"
    assert AssignUserToBucket(userId, salt, 1) == 0, "Should be 0"

def test12():
    """0 buckets
    """
    userId = "25HE93HQ635J131GDHWPQ98Y3215G12G"
    salt =   "ASHT83761GH104H1-1EB1HJL37312B35"
    assert AssignUserToBucket(userId, salt, 0) == -1, "Should be -1"