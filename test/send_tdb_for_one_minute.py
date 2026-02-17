"""分境界開始で1分間テストセンテンスを送信する手動テストスクリプト。"""

from __future__ import annotations

import logging
import socket
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

LOGGER = logging.getLogger(__name__)

SEND_DURATION_SECONDS = 60
SEND_INTERVAL_SECONDS = 1
TEST_SENTENCES = [
    "!AIVDM,1,1,0,A,32vg20OP00:4r2jEq7s<J?vr231Q,0*63",
    "!AIVDM,1,1,1,B,32vg20OP00b4r2fEq7sLJ?w>22k1,0*33",
    "!VATDB,2,2,2,200000001,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!AIVDM,1,1,3,A,32vg20OP00:4r2fEq7rtJ?wR22uQ,0*40",
    "!VATDB,2,1,4,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!AIVDM,1,1,5,B,32vg20OP00:4r2dEq7r<J?wl2361,0*14",
    "!AIVDM,1,1,6,A,32vg20OP00:4r2VEq7qLJ7vD2001,0*7D",
    "!AIVDM,1,1,7,B,32vg20OP00:4r2NEq7qLJ?vV2001,0*75",
    "!VATDB,2,2,8,200000001,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!AIVDM,1,1,9,A,12vg20OP00:4r2BEq7o<J?vr2D00,0*41",
    "!AIVDM,1,1,10,B,12vg20OP00:4r2BEq7ntJ?w<2L00,0*4B",
    "!VATDB,2,2,11,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!AIVDM,1,1,12,A,12vg20OP00:4r2BEq7o<J?wP2<00,0*1C",
    "!VETDB,1,1,13,200000001,200000002,,0,VDE:TESTIND,2*51",
    "!VETDB,1,1,14,200000001,200000002,,0,VDE:TESTIND,2*62",
    "!VETDB,1,1,15,200000001,200000002,,0,VDE:TESTIND,2*63",
    "!VATDB,2,1,16,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!VATDB,2,1,17,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!VATDB,2,2,18,200000001,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!AIVDM,1,1,19,A,32vg20OP00:4r2jEq7s<J?vr231Q,0*63",
    "!AIVDM,1,1,20,B,32vg20OP00b4r2fEq7sLJ?w>22k1,0*33",
    "!VATDB,2,2,21,200000001,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!AIVDM,1,1,22,A,32vg20OP00:4r2fEq7rtJ?wR22uQ,0*40",
    "!VATDB,2,1,23,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!AIVDM,1,1,24,B,32vg20OP00:4r2dEq7r<J?wl2361,0*14",
    "!AIVDM,1,1,25,A,32vg20OP00:4r2VEq7qLJ7vD2001,0*7D",
    "!AIVDM,1,1,26,B,32vg20OP00:4r2NEq7qLJ?vV2001,0*75",
    "!VATDB,2,2,27,200000001,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!AIVDM,1,1,28,A,12vg20OP00:4r2BEq7o<J?vr2D00,0*41",
    "!AIVDM,1,1,29,B,12vg20OP00:4r2BEq7ntJ?w<2L00,0*4B",
    "!VATDB,2,2,30,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!AIVDM,1,1,31,A,12vg20OP00:4r2BEq7o<J?wP2<00,0*1C",
    "!VETDB,1,1,32,200000001,200000002,,0,VDE:TESTIND,2*51",
    "!VETDB,1,1,33,200000001,200000002,,0,VDE:TESTIND,2*62",
    "!VETDB,1,1,34,200000001,200000002,,0,VDE:TESTIND,2*63",
    "!VATDB,2,1,35,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!VATDB,2,1,36,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!VATDB,2,2,37,200000001,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!AIVDM,1,1,38,A,32vg20OP00:4r2jEq7s<J?vr231Q,0*63",
    "!AIVDM,1,1,39,B,32vg20OP00b4r2fEq7sLJ?w>22k1,0*33",
    "!VATDB,2,2,40,200000001,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!AIVDM,1,1,41,A,32vg20OP00:4r2fEq7rtJ?wR22uQ,0*40",
    "!VATDB,2,1,42,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!AIVDM,1,1,43,B,32vg20OP00:4r2dEq7r<J?wl2361,0*14",
    "!AIVDM,1,1,44,A,32vg20OP00:4r2VEq7qLJ7vD2001,0*7D",
    "!AIVDM,1,1,45,B,32vg20OP00:4r2NEq7qLJ?vV2001,0*75",
    "!VATDB,2,2,46,200000001,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!AIVDM,1,1,47,A,12vg20OP00:4r2BEq7o<J?vr2D00,0*41",
    "!AIVDM,1,1,48,B,12vg20OP00:4r2BEq7ntJ?w<2L00,0*4B",
    "!VATDB,2,2,49,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!AIVDM,1,1,50,A,12vg20OP00:4r2BEq7o<J?wP2<00,0*1C",
    "!VETDB,1,1,51,200000001,200000002,,0,VDE:TESTIND,2*51",
    "!VETDB,1,1,52,200000001,200000002,,0,VDE:TESTIND,2*62",
    "!VETDB,1,1,53,200000001,200000002,,0,VDE:TESTIND,2*63",
    "!VATDB,2,1,54,200000005,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!VATDB,2,1,55,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
    "!VATDB,2,2,56,200000005,200000002,0,0,qrN1pp6Opp6Q@nuQLoAQK21<JFqcpp6gpp21pp6Mpp6f8fBuVNNIgB;SPa;SPJCSPJcSPHCSPJOSPHCSPHwSP88tC4Hvr8Rqpp6`qcRgpp21rIVppp6`qcFopp21pp6Mpp6Gpp6Vq;brpp6`q;brpp22?4i6?fFSd>>1S>FiRf>1TN>1d>FAgN>1S>FfR>>2RN>2S>>0PNJ3QNFPdN>1S>>1a>>1bf>1S>>2S>>1d>JLbfJMaN>1S>F;UN>1SNF7ff>1VN>0PSi<ASptC4HvqcFopp:BqpVqqHRUpp6bqJ2lq`V0pp6Wpp6gpp6bpp6?pp21r:bhpp:2pp6<qJr9qKv3pp6Gpp6Vq`jApp:1pp:;qJ2lq`V0pp6cpp22?4i6?fFea>NcRv>2TfNmPf>2Sv>2RN>1Vv>0PNFpS>JLVv>1S>N1cv>2Rv>2a>>3dv>3UN>3bN>2TfNgRN>1Q>>1af>1Q>>1Sv>0PSi<ASptC4Hvr:J;pp68pp6bpp64q;RFqqF<pp:Bpp21r:J;pp68pp:;rIJRq;v2pp6ppp22?4i6?fNWPN>1Wv>1`N>1cv>0PN>1WN>2Tv>1bfNTgfBtVf>1cfFfWvN?gf>2TfBw`N>1V>>1af>1Q>>1gf>1VN>0PP,4*17",
    "!VETDB,1,1,57,200000001,200000002,,0,VDE:TESTIND,2*51",
    "!VETDB,1,1,58,200000001,200000002,,0,VDE:TESTIND,2*62",
    "!VATDB,2,1,59,200000001,200000002,0,0,q;RFqqF<pp6fq;nIqqVupp6cpp21?4i6?fBrff>1cfF?cvR3gNJ0av>2Tf>1df>2RN>1Sv>0PSi<ASsVeKOSPJsTf8cSPJgSPJwSP87SPKsSPJ3UdHcSPHgSPJcSPHCVPs?SPHCSPHkSPH;SP`gSP88tC4Hvpp6Mpp6fqKbVpp6cq`>5qJ2ipp6grH2DqHR7pp:<pp21qHRTqaJepp6gqJnTqrf;pp6Ipp:;pp22?4i6?f>1WN>2S>>1cv>0PN>3Qf>2cv>3Sf>3cN>2f>>3g>>1S>>1gf>1`>FiRf>1Q>>1af>1Q>>1bf>1Q28o<>vtQN>1cfBpUfNES2;SPJsWSssUcawSPJ3SP88tC4Hv?4i6?f>1WN>2S>>1av>2Pf>0PNBrff>1cvJmev>2TfJp`N>2Rv>0PSi<ASsTfqGTf`gSPJsSPIwSP`7SPJgSP87VVbsSP`WSPIOSPJsSPIwSP`7SPJgSP87`bc3SPHgSPa;Uc`SSP`gSPIwSP`7SPJgSP88tC4Hv?4i6?P,4*26",
]


# 補助処理
def _load_receive_destination() -> tuple[str, int]:
    """受信設定から送信先マルチキャスト情報を読み込む。

    引数:
        なし。

    戻り値:
        送信先マルチキャストIPとポート番号。

    例外:
        ModuleNotFoundError: `src`配下モジュールの読み込みに失敗した場合。
    """
    project_root = Path(__file__).resolve().parent.parent
    src_directory = project_root / "src"
    src_directory_text = str(src_directory)

    if src_directory_text not in sys.path:
        sys.path.insert(0, src_directory_text)

    from consts.receive_constants import RECEIVE_MULTICAST_IP, RECEIVE_PORT

    return RECEIVE_MULTICAST_IP, RECEIVE_PORT


# 補助処理
def _resolve_next_minute_boundary_utc(now_utc: datetime) -> datetime:
    """現在時刻から次の分境界UTC時刻を求める。

    引数:
        now_utc: 現在時刻として扱うUTC日時。

    戻り値:
        次の分境界UTC時刻。

    例外:
        なし。
    """
    if now_utc.tzinfo is None:
        normalized_utc = now_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = now_utc.astimezone(UTC)

    current_minute_floor = normalized_utc.replace(second=0, microsecond=0)
    return current_minute_floor + timedelta(minutes=1)


# 補助処理
def _sleep_until_utc(target_utc: datetime) -> None:
    """指定UTC時刻になるまで待機する。

    引数:
        target_utc: 待機終了時刻のUTC日時。

    戻り値:
        なし。

    例外:
        なし。
    """
    if target_utc.tzinfo is None:
        normalized_target_utc = target_utc.replace(tzinfo=UTC)
    else:
        normalized_target_utc = target_utc.astimezone(UTC)

    while True:
        remaining_seconds = (normalized_target_utc - datetime.now(UTC)).total_seconds()
        if remaining_seconds <= 0:
            break

        # 境界直前のずれを減らすため、短い単位で分割して待機する。
        time.sleep(min(remaining_seconds, 0.2))


# 補助処理
def _format_utc_text(timestamp_utc: datetime) -> str:
    """UTC日時をログ向け文字列へ変換する。

    引数:
        timestamp_utc: 変換対象のUTC日時。

    戻り値:
        `YYYY-MM-DDTHH:MM:SS.ffffffZ`形式の文字列。

    例外:
        なし。
    """
    if timestamp_utc.tzinfo is None:
        normalized_utc = timestamp_utc.replace(tzinfo=UTC)
    else:
        normalized_utc = timestamp_utc.astimezone(UTC)

    return normalized_utc.isoformat(timespec="microseconds").replace("+00:00", "Z")


# 補助処理
def _resolve_test_sentence(sequence: int) -> str:
    """送信連番に対応するテストセンテンスを返す。

    引数:
        sequence: 送信連番（0始まり）。

    戻り値:
        送信対象センテンス文字列。

    例外:
        ValueError: 送信連番が負数の場合。
    """
    if sequence < 0:
        raise ValueError(f"送信連番は0以上で指定してください。sequence={sequence}")

    return TEST_SENTENCES[sequence % len(TEST_SENTENCES)]


# メイン処理
def send_tdb_for_one_minute() -> None:
    """分境界を起点に1分間、1秒間隔でテストセンテンスを送信する。

    引数:
        なし。

    戻り値:
        なし。

    例外:
        OSError: UDP送信処理に失敗した場合。
    """
    multicast_ip, port = _load_receive_destination()
    next_minute_boundary_utc = _resolve_next_minute_boundary_utc(datetime.now(UTC))

    LOGGER.info(
        "開始: 分境界送信テストを開始します。送信先=%s:%d 分境界=%s",
        multicast_ip,
        port,
        _format_utc_text(next_minute_boundary_utc),
    )

    _sleep_until_utc(next_minute_boundary_utc)
    measurement_end_utc = next_minute_boundary_utc + timedelta(seconds=SEND_DURATION_SECONDS)
    LOGGER.info("開始: 分境界に到達したためテストセンテンス送信を開始します。送信期間=%d秒", SEND_DURATION_SECONDS)

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as udp_socket:
        udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        udp_socket.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)

        sequence = 0
        sent_count = 0
        while True:
            scheduled_send_utc = next_minute_boundary_utc + timedelta(seconds=sequence * SEND_INTERVAL_SECONDS)
            if scheduled_send_utc >= measurement_end_utc:
                break
            _sleep_until_utc(scheduled_send_utc)

            test_sentence = _resolve_test_sentence(sequence)
            udp_socket.sendto(test_sentence.encode("utf-8"), (multicast_ip, port))
            sent_count += 1
            LOGGER.info(
                "終了: テストセンテンス送信が完了しました。連番=%02d 送信時刻=%s データ=%s",
                sequence + 1,
                _format_utc_text(datetime.now(UTC)),
                test_sentence,
            )
            sequence += 1

    LOGGER.info("終了: 分境界送信テストが完了しました。送信件数=%d", sent_count)


def main() -> None:
    """テストスクリプトの実行入口。"""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")

    try:
        send_tdb_for_one_minute()
    except KeyboardInterrupt:
        LOGGER.info("終了: Ctrl+Cを受け付けたため分境界送信テストを終了します。")
    except Exception:
        LOGGER.exception("異常: 分境界送信テストで例外が発生しました。")
        raise


if __name__ == "__main__":
    main()
