
// NOTE jvs 30-June-2004:  This is a kludge for the problem
// with boost::lexical_cast<int>(std::string) on mingw.
// I have submitted a bug report to STLport; maybe they'll
// incorporate it so we don't have to do this.

_STLP_BEGIN_NAMESPACE

static void instantiate_ws()
{
    basic_istream<char, char_traits<char> > *pis;
    ws(*pis);
}

_STLP_END_NAMESPACE
