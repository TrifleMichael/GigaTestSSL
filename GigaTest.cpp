#include <curl/curl.h>
#include <unordered_map>
#include <stdio.h>
#include <stdlib.h>
#include <uv.h>
#include <curl/curl.h>
#include <string>
#include <iostream>
#include <thread>     // get_id
#include <vector>
#include <condition_variable>
#include <mutex>

#include <chrono>   // time measurement
// #include <unistd.h> // time measurement

#include <sys/types.h> /* See NOTES */
#include <sys/socket.h>
#include "GigaTest.h"

std::string pathsCS = "https://alice-ccdb.cern.ch/CTP/Calib/CTFDictionary/1626472048000,https://alice-ccdb.cern.ch/ZDC/Config/Module/1546300800000,https://alice-ccdb.cern.ch/ITS/Config/AlpideParam/1657442962000,https://alice-ccdb.cern.ch/TPC/Calib/TimeGain/0,https://alice-ccdb.cern.ch/TPC/Calib/Temperature/1659949565413,https://alice-ccdb.cern.ch/CPV/PedestalRun/HighPedChannels/1659935186741,https://alice-ccdb.cern.ch/FT0/Calib/ChannelTimeOffset/1546300800000,https://alice-ccdb.cern.ch/TOF/Calib/LHCphase/0,https://alice-ccdb.cern.ch/TOF/Calib/ChannelCalib/1654242778226,https://alice-ccdb.cern.ch/ITS/Calib/CTFDictionary/1653192000000,https://alice-ccdb.cern.ch/MFT/Calib/CTFDictionary/1653192000000,https://alice-ccdb.cern.ch/MCH/Align/1638479347757,https://alice-ccdb.cern.ch/ITS/Calib/Align/1635199200000,https://alice-ccdb.cern.ch/MID/Config/DCSDPconfig/1653499219078,https://alice-ccdb.cern.ch/ITS/Align/1635322620830,https://alice-ccdb.cern.ch/ITS/Calib/ClusterDictionary/1653192000000,https://alice-ccdb.cern.ch/MFT/Calib/DeadMap/0,https://alice-ccdb.cern.ch/TRD/Calib/Align/1,https://alice-ccdb.cern.ch/EMC/Config/CalibParam/1658945928000,https://alice-ccdb.cern.ch/PHS/Calib/BadMap/1,https://alice-ccdb.cern.ch/FV0/Align/1638479347757,https://alice-ccdb.cern.ch/FDD/Config/DCSDPconfig/1,https://alice-ccdb.cern.ch/MID/Align/1638479347757,https://alice-ccdb.cern.ch/EMC/Config/ChannelScaleFactors/1546300800000,https://alice-ccdb.cern.ch/MID/Calib/CTFDictionary/1626472048000,https://alice-ccdb.cern.ch/GLO/Config/EnvVars/1659949276728,https://alice-ccdb.cern.ch/EMC/Calib/Temperature/1659949599544,https://alice-ccdb.cern.ch/TOF/Calib/LVStatus/1659621343526,https://alice-ccdb.cern.ch/GLO/Config/Collimators/1659949276728,https://alice-ccdb.cern.ch/GLO/Config/LHCIFDataPoints/1659949276728,https://alice-ccdb.cern.ch/MFT/Config/Params/1659947087892,https://alice-ccdb.cern.ch/CPV/PedestalRun/FEEThresholds/1659935186741,https://alice-ccdb.cern.ch/MCH/Calib/BadChannel/1659935281422,https://alice-ccdb.cern.ch/MFT/Calib/NoiseMap/1659890496672,https://alice-ccdb.cern.ch/MFT/Condition/DCSDPs/1659949674726,https://alice-ccdb.cern.ch/TPC/Calib/Gas/1659949565413,https://alice-ccdb.cern.ch/GLO/Config/GRPLHCIF/1659937041924,https://alice-ccdb.cern.ch/TOF/Calib/Diagnostic/1,https://alice-ccdb.cern.ch/TRD/Calib/DCSDPsU/1659949587465,https://alice-ccdb.cern.ch/TPC/Calib/Pulser/1659936185889,https://alice-ccdb.cern.ch/CTP/Calib/Scalers/1659946806420,https://alice-ccdb.cern.ch/TPC/Calib/PedestalNoise/1659935701974,https://alice-ccdb.cern.ch/MCH/BadChannelCalib/1652347115925,https://alice-ccdb.cern.ch/TPC/Calib/CE/1659890954106,https://alice-ccdb.cern.ch/CPV/Calib/Pedestals/1659935186741,https://alice-ccdb.cern.ch/GLO/Config/GRPMagField/1659621342493,https://alice-ccdb.cern.ch/MID/Calib/Align/1,https://alice-ccdb.cern.ch/FT0/Calib/DCSDPs/1659949426082,https://alice-ccdb.cern.ch/GLO/GRP/BunchFilling/1657583876199,https://alice-ccdb.cern.ch/TPC/Align/1638479347757,https://alice-ccdb.cern.ch/TOF/Config/DCSDPconfig/1652733375051,https://alice-ccdb.cern.ch/MFT/Calib/Align/1635322620830,https://alice-ccdb.cern.ch/TOF/Calib/DCSDPs/1659949388995,https://alice-ccdb.cern.ch/MFT/Align/1635322620830,https://alice-ccdb.cern.ch/TRD/Calib/DCSDPsI/1659949587465,https://alice-ccdb.cern.ch/TPC/Calib/HV/1659949565413,https://alice-ccdb.cern.ch/GRP/Config/DCSDPconfig/1651753934367,https://alice-ccdb.cern.ch/MCH/Calib/CTFDictionary/1626472048000,https://alice-ccdb.cern.ch/CPV/Calib/BadChannelMap/1659935743007,https://alice-ccdb.cern.ch/TRD/Calib/ChamberStatus/1,https://alice-ccdb.cern.ch/PHS/Align/1638479347757,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Krypton_2015-01/1,https://alice-ccdb.cern.ch/ITS/Calib/DeadMap/0,https://alice-ccdb.cern.ch/PHS/Config/RecoParams/1657915200001,https://alice-ccdb.cern.ch/FT0/Align/1638479347757,https://alice-ccdb.cern.ch/FV0/Calibration/ChannelTimeOffset/1546300800000,https://alice-ccdb.cern.ch/IT3/Align/1638479347757,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Krypton_2011-02/1,https://alice-ccdb.cern.ch/PHS/Calib/CTFDictionary/1653192000000,https://alice-ccdb.cern.ch/FCT/Calib/Align/1,https://alice-ccdb.cern.ch/ZDC/Calib/RecoConfigZDC/1546300800000,https://alice-ccdb.cern.ch/TRK/Align/1638479347757,https://alice-ccdb.cern.ch/EMC/Calib/Align/1,https://alice-ccdb.cern.ch/ITS/Config/ClustererParam/1657726378777,https://alice-ccdb.cern.ch/EMC/Align/1638479347757,https://alice-ccdb.cern.ch/CPV/Config/CPVSimParams/946684800000,https://alice-ccdb.cern.ch/EMC/Calib/CTFDictionary/1653192000000,https://alice-ccdb.cern.ch/TST/Calib/Align/1,https://alice-ccdb.cern.ch/PHS/BadMap/Ped/1,https://alice-ccdb.cern.ch/IT3/Calib/Align/1,https://alice-ccdb.cern.ch/FV0/Calib/Align/1640991600000,https://alice-ccdb.cern.ch/FDD/Calib/CTFDictionary/1655282522000,https://alice-ccdb.cern.ch/TRD/Calib/PadStatus/1,https://alice-ccdb.cern.ch/FDD/Calib/Align/1,https://alice-ccdb.cern.ch/FT0/Config/DCSDPconfig/1,https://alice-ccdb.cern.ch/PHS/BadMap/1,https://alice-ccdb.cern.ch/CPV/Calib/Gains/1627542202422,https://alice-ccdb.cern.ch/TRD/Calib/NoiseMapMCM/1,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Krypton_2018-01/1,https://alice-ccdb.cern.ch/TPC/Config/FEEPad/0,https://alice-ccdb.cern.ch/MFT/Config/DCSDPconfig/1653494821018,https://alice-ccdb.cern.ch/MCH/Config/DCSDPconfig/1653499164150,https://alice-ccdb.cern.ch/MFT/Calib/ClusterDictionary/1653192000000,https://alice-ccdb.cern.ch/HMP/Calib/CTFDictionary/1626472048000,https://alice-ccdb.cern.ch/PHS/BadMap/Chi/1,https://alice-ccdb.cern.ch/TRK/Calib/Align/1,https://alice-ccdb.cern.ch/PHS/BadMap/Occ/1,https://alice-ccdb.cern.ch/EMC/Calib/BadChannelMap/1626472048000,https://alice-ccdb.cern.ch/FT3/Align/1638479347757,https://alice-ccdb.cern.ch/CPV/Config/CPVCalibParams/1653639365626,https://alice-ccdb.cern.ch/FT0/Config/LookupTable/1654105278523,https://alice-ccdb.cern.ch/TOF/Calib/CTFDictionary/1626472048000,https://alice-ccdb.cern.ch/TRD/Calib/DCSDPs/1653646795136,https://alice-ccdb.cern.ch/TRD/Config/DCSDPconfig/1654615142813,https://alice-ccdb.cern.ch/MFT/Config/ClustererParam/1657726413855,https://alice-ccdb.cern.ch/FT0/Calibration/ChannelTimeOffset/1546300800000,https://alice-ccdb.cern.ch/TRD/Calib/HalfChamberStatusQC/1577833200000,https://alice-ccdb.cern.ch/TRD/Calib/PadNoise/1,https://alice-ccdb.cern.ch/TPC/Config/DCSDPconfig/1650631890013,https://alice-ccdb.cern.ch/CPV/Calib/Align/1,https://alice-ccdb.cern.ch/FT3/Calib/Align/1,https://alice-ccdb.cern.ch/FV0/Calib/ChannelTimeOffset/1546300800000,https://alice-ccdb.cern.ch/CPV/Calib/CTFDictionary/1653192000000,https://alice-ccdb.cern.ch/FT0/Calib/Align/1640991600000,https://alice-ccdb.cern.ch/TRD/Calib/CTFDictionary/1626472048000,https://alice-ccdb.cern.ch/TPC/Calib/VDriftTgl/1,https://alice-ccdb.cern.ch/MFT/Config/AlpideParam/1657663439000,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Krypton_2015-02/1,https://alice-ccdb.cern.ch/TOF/Calib/HVStatus/1659621342493,https://alice-ccdb.cern.ch/ITS/Calib/NoiseMap/0,https://alice-ccdb.cern.ch/FDD/Config/LookupTable/1654105278523,https://alice-ccdb.cern.ch/HMP/Align/1638479347757,https://alice-ccdb.cern.ch/FV0/Config/DCSDPconfig/1,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Krypton_2012-01/1,https://alice-ccdb.cern.ch/PHS/Calib/CalibParams/1651356383449,https://alice-ccdb.cern.ch/FDD/Align/1638479347757,https://alice-ccdb.cern.ch/TRD/TrapConfig/cf_pg-fpnp32_zs-s16-deh_tb30_trkl-b5n-fs1e24-ht200-qs0e24s24e23-pidlinear-pt100_ptrg.r5549/1,https://alice-ccdb.cern.ch/ZDC/Calib/TDCCalib/1546300800000,https://alice-ccdb.cern.ch/EMC/Calib/TimeCalibParams/1626472048000,https://alice-ccdb.cern.ch/ITS/DCS_CONFIG/1659532359719,https://alice-ccdb.cern.ch/CPV/Align/1638479347757,https://alice-ccdb.cern.ch/CTP/Config/TriggerOffsets/1,https://alice-ccdb.cern.ch/TRD/Calib/LocalGainFactor/1,https://alice-ccdb.cern.ch/FT0/Calib/CTFDictionary/1655282522000,https://alice-ccdb.cern.ch/ZDC/Calib/InterCalibConfig/1546300800000,https://alice-ccdb.cern.ch/EMC/Temperature/2254,https://alice-ccdb.cern.ch/PHS/Calib/Align/1,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Krypton_2011-03/1,https://alice-ccdb.cern.ch/FV0/Calib/CTFDictionary/1655282522000,https://alice-ccdb.cern.ch/HMP/Calib/Align/1,https://alice-ccdb.cern.ch/ACO/Align/1638479347757,https://alice-ccdb.cern.ch/GLO/Calib/MeanVertex/1635257044785,https://alice-ccdb.cern.ch/EMC/FeeDCS/1653646593677,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Gaintbl_Uniform_FGAN0_2012-01/1,https://alice-ccdb.cern.ch/MCH/Calib/Align/1,https://alice-ccdb.cern.ch/GRP/StartOrbit/0,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Krypton_2011-01/1,https://alice-ccdb.cern.ch/EMC/Config/DCSDPconfig/1647423729718,https://alice-ccdb.cern.ch/TPC/Calib/Align/1,https://alice-ccdb.cern.ch/TRD/OnlineGainTables/Gaintbl_Uniform_FGAN0_2011-01/1,https://alice-ccdb.cern.ch/TPC/Calib/CTFDictionary/1626472048000,https://alice-ccdb.cern.ch/EMC/Calib/FeeDCS/1659690910837,https://alice-ccdb.cern.ch/GLO/Config/GRPECS/1659887718188,https://alice-ccdb.cern.ch/TOF/Calib/Align/1,https://alice-ccdb.cern.ch/TOF/Align/1638479347757,https://alice-ccdb.cern.ch/FV0/Config/LookupTable/1651241791741,https://alice-ccdb.cern.ch/ZDC/Calib/CTFDictionary/1626472048000,https://alice-ccdb.cern.ch/ZDC/Calib/IntegrationParam/1635570656996,https://alice-ccdb.cern.ch/ZDC/Calib/EnergyCalib/1546300800000,https://alice-ccdb.cern.ch/ZDC/Calib/Align/1,https://alice-ccdb.cern.ch/ZDC/Calib/BaselineCalib/1546300800000,https://alice-ccdb.cern.ch/ZDC/Align/1638479347757,https://alice-ccdb.cern.ch/ZDC/Calib/TowerCalib/1546300800000,https://alice-ccdb.cern.ch/ZDC/Calib/TDCCorr/1546300800000,https://alice-ccdb.cern.ch/MID/Calib/BadChannels/1659890536060,https://alice-ccdb.cern.ch/ZDC/Config/Sim/1546300800000,https://alice-ccdb.cern.ch/CTP/Config/Config/1659946806420,https://alice-ccdb.cern.ch/TPC/Calib/PadGainResidual/0,https://alice-ccdb.cern.ch/PHS/Calib/Pedestals/1657807103014,https://alice-ccdb.cern.ch/TPC/Calib/PadGainFull/0,https://alice-ccdb.cern.ch/TPC/Calib/LaserTracks/1659513086496,https://alice-ccdb.cern.ch/TRD/Calib/ChamberCalibrations/1,https://alice-ccdb.cern.ch/TRD/Align/1638479347757,https://alice-ccdb.cern.ch/ITS/Calib/Confdbmap/1451606461000,https://alice-ccdb.cern.ch/CTP/Calib/Align/1,https://alice-ccdb.cern.ch/CPV/PhysicsRun/GainCalibData/946684800000,https://alice-ccdb.cern.ch/TRD/Calib/CalVdriftExB/1,https://alice-ccdb.cern.ch/TPC/Calib/TopologyGain/0,https://alice-ccdb.cern.ch/GLO/Param/MatLUT/1636019021295,https://alice-ccdb.cern.ch/GLO/Config/Geometry/1546300800000,https://alice-ccdb.cern.ch/GLO/Config/GeometryAligned/1640991600000,https://alice-ccdb.cern.ch/CTP/Calib/OrbitReset/1659938355618,https://alice-ccdb.cern.ch/TOF/Calib/FEELIGHT/1659947098000,https://alice-ccdb.cern.ch/TRD/Calib/DCSDPsGas/1659949080207,https://alice-ccdb.cern.ch/CPV/PedestalRun/DeadChannels/1659935186741,https://alice-ccdb.cern.ch/CPV/PedestalRun/ChannelEfficiencies/1659935186741";
std::string etagsCS = "f75d650f-c805-11ec-b790-2a010e0a0b16,c4613b4a-0439-11ed-8000-200114580202,5f2dc7de-003f-11ed-a97d-2a010e0a0b16,fc6eb7f0-a46e-11ec-982e-0aa2043e1b9a,08c47e86-16fa-11ed-8002-0aa1229c1b9a,db326e41-16d7-11ed-8002-0aa202a21b9a,756d3f8b-d5cd-11ec-9222-200114580d00,33982998-52a8-11ec-bf8f-0aa2041d1b9a,c8a42774-0e73-11ed-8000-200114580202,1d67e544-d92e-11ec-9e90-2a010e0a0b16,0511e1a6-d92e-11ec-9e90-2a010e0a0b16,38181fa0-52eb-11ec-8357-2a010e0a0b16,a8bc88fb-ec84-11ec-8c09-2a010e0a0b16,f3a71542-dc4e-11ec-9e90-2a010e0a0b16,5516eb40-a543-11ec-975c-25fc52e124ee,e1b7711f-d92a-11ec-8eb6-200114580202,4eca84a0-a047-11ec-8357-25fc52e1250c,f82105ae-bc3e-11ec-b66d-2a010e0a0b16,162e7e03-1a3a-11ed-b286-200114580202,a5b6f9bf-9b39-11ec-982e-0aa202a21b9a,38a32190-52eb-11ec-8357-2a010e0a0b16,9be86b16-0170-11ed-a336-2a010e0a0b16,383b8620-52eb-11ec-8357-2a010e0a0b16,27dec102-13e7-11ed-8000-200114580d00,d7c41e60-515f-11ec-975c-2a010e0a0b16,0eab9fc9-16fa-11ed-8002-0aa1229c1b9a,69c80f1a-16f9-11ed-8002-0aa1229c1b9a,10a14683-17ce-11ed-8002-0aa1229c1b9a,0eb1ed91-16fa-11ed-8002-0aa1229c1b9a,00ed5e2a-16fa-11ed-8002-0aa1229c1b9a,90b44c90-16f3-11ed-8002-0aa1229c1b9a,db3a4589-16d7-11ed-8002-0aa202a21b9a,1a17f296-16d8-11ed-8002-0aa200321b9a,ce825c19-166f-11ed-8002-0aa200321b9a,96942d4c-16f9-11ed-8002-0aa1229c1b9a,08bc0570-16fa-11ed-8002-0aa1229c1b9a,2cd79ee5-16dc-11ed-8002-0aa1229c1b9a,19889fe2-b96e-11ec-b66d-200114580202,a9b7bce3-16f9-11ed-8002-0aa1229c1b9a,2eb7b048-16da-11ed-8002-0aa204151b9a,db92a6bd-16f5-11ed-8002-0aa140651b9a,0e47ab39-16d9-11ed-8002-0aa204151b9a,cb6d771c-d1d4-11ec-8000-0aa200321b9a,de66bf44-1670-11ed-8002-0aa2040e1b9a,db3e6115-16d7-11ed-8002-0aa202a21b9a,218d3bd3-13fd-11ed-8002-0aa1229c1b9a,f938e7ba-bc3e-11ec-b66d-2a010e0a0b16,67bcba6d-16fa-11ed-8002-0aa1229c1b9a,48f3aa8f-0175-11ed-8000-0aa2040a1b9a,36eebad0-52eb-11ec-8357-2a010e0a0b16,d4cc38a5-d557-11ec-8000-0aa1229c1b9a,a974ae51-bc3f-11ec-b66d-2a010e0a0b16,51969d3e-16fa-11ed-8002-0aa1229c1b9a,30795be0-a3c4-11ec-975c-25fc52e124ee,a9b2f465-16f9-11ed-8002-0aa1229c1b9a,08c06681-16fa-11ed-8002-0aa1229c1b9a,64acb793-cc6f-11ec-981d-200114580202,d5e52530-515f-11ec-975c-2a010e0a0b16,26aa11b2-16d9-11ed-8002-0aa202a21b9a,e724d29a-d22d-11ec-8dd8-511cc1ec24ee,3760dd90-52eb-11ec-8357-2a010e0a0b16,668fd370-5488-11ec-975c-2a01cb15032a,57a0a230-a047-11ec-8357-25fc52e1250c,f96ba879-0613-11ed-8002-0aa202a21b9a,3880cc80-52eb-11ec-8357-2a010e0a0b16,b54b6ad5-d832-11ec-9e90-200114580202,390e8d90-52eb-11ec-8357-2a010e0a0b16,64fb29b0-5488-11ec-975c-2a01cb15032a,70196a15-d940-11ec-9e90-2a010e0a0b16,37d89e41-d043-11ec-981d-2a010e0a0b16,abbc3456-cca5-11ec-b790-200114580d00,393157d0-52eb-11ec-8357-2a010e0a0b16,f8ad3060-bc3e-11ec-b66d-2a010e0a0b16,13286527-02c1-11ed-8000-200114580202,37ac8c90-52eb-11ec-8357-2a010e0a0b16,5ce9988b-d60c-11ec-8000-0aa202a21b9a,4904c8f8-d92f-11ec-9e90-2a010e0a0b16,f9e54f0f-bc3e-11ec-b66d-2a010e0a0b16,73f992a0-b1d5-11ec-a6f9-0aa202a21b9a,fa07d9ff-bc3e-11ec-b66d-2a010e0a0b16,38865750-e310-11ec-9e90-2a010e0a0b16,f4683693-ec97-11ec-bea3-2a010e0a0b16,e8418bc7-d22d-11ec-8dd8-511cc1ec24ee,f9c530b8-bc3e-11ec-b66d-2a010e0a0b16,386b34f9-fc71-11ec-a336-200114580204,4fe5e667-9b14-11ec-982e-0aa202a21b9a,fdbfdde0-5441-11ec-8357-200114580202,e871e7e5-d22d-11ec-8dd8-511cc1ec24ee,680eab40-5488-11ec-975c-2a01cb15032a,dcf98353-a46e-11ec-982e-0aa2043e1b9a,b6208708-dc44-11ec-8eb6-2a010e0a0b16,d2f69665-dc4e-11ec-9e90-2a010e0a0b16,d9d33094-d92a-11ec-8eb6-200114580202,e6656370-515f-11ec-975c-2a010e0a0b16,73f9080e-b1d5-11ec-a6f9-0aa202a21b9a,fa2ad4cb-bc3e-11ec-b66d-2a010e0a0b16,73f8583d-b1d5-11ec-a6f9-0aa202a21b9a,cdca72f0-a944-11ec-975c-25fc52e124ee,39542210-52eb-11ec-8357-2a010e0a0b16,41410990-dd95-11ec-8000-0aa202a21b9a,a86e62a9-ee4d-11ec-841e-200114580202,d98e3000-515f-11ec-975c-2a010e0a0b16,8d982d22-dda6-11ec-8000-0aa1229c1b9a,29cf3933-e675-11ec-9e90-200114580202,280dd2ac-02c1-11ed-8000-200114580202,0e34e534-d5cd-11ec-90c5-200114580d00,50eca180-f22b-11ec-b9c0-200114580202,e81c639a-d22d-11ec-8dd8-511cc1ec24ee,ee1b51f7-c23a-11ec-8000-0aa2049b1b9a,f88944a8-bc3e-11ec-b66d-2a010e0a0b16,fa4d6144-bc3e-11ec-b66d-2a010e0a0b16,ea2bed9b-d832-11ec-8eb6-200114580202,74f5dcc0-d92f-11ec-9e90-2a010e0a0b16,3f67485b-e310-11ec-9e90-2a010e0a0b16,e0ebde60-515f-11ec-975c-2a010e0a0b16,5497bcc3-0843-11ed-8000-200114580202,2f375991-023d-11ed-8000-2a010e0a0b16,674fc810-5488-11ec-975c-2a01cb15032a,215b3c15-13fd-11ed-8002-0aa1229c1b9a,5cc82830-9fae-11ec-8357-25fc52e1250c,975c780e-ee4d-11ec-841e-200114580202,37ceba90-52eb-11ec-8357-2a010e0a0b16,a43b9d43-0170-11ed-a336-2a010e0a0b16,65f1beb0-5488-11ec-975c-2a01cb15032a,2a114c11-c8d2-11ec-8000-0aa2041a1b9a,38c79980-52eb-11ec-8357-2a010e0a0b16,e6c280c9-d22d-11ec-8dd8-511cc1ec24ee,0bc5c80f-cca6-11ec-b790-200114580d00,ebed4af0-a944-11ec-975c-25fc52e124ee,f3820402-132d-11ed-8002-0aa1229c1b9a,37872a40-52eb-11ec-8357-2a010e0a0b16,907486a2-ecaa-11ec-bea3-2a010e0a0b16,e7a02548-d22d-11ec-8dd8-511cc1ec24ee,06548824-ec98-11ec-bea3-2a010e0a0b16,6cfda43e-cca5-11ec-b790-200114580d00,9f3f7140-dda7-11ec-8000-0aa1229c1b9a,f86688b4-bc3e-11ec-b66d-2a010e0a0b16,656eac00-5488-11ec-975c-2a01cb15032a,120b9920-ec98-11ec-bea3-2a010e0a0b16,f8d05560-bc3e-11ec-b66d-2a010e0a0b16,38ea15a0-52eb-11ec-8357-2a010e0a0b16,ad65fd5a-1fa1-11ed-a788-200114580202,16ac6d3b-dda6-11ec-8000-0aa1229c1b9a,68e75800-5488-11ec-975c-2a01cb15032a,f9160ba9-bc3e-11ec-b66d-2a010e0a0b16,7cb85e90-9e2c-11ec-8357-90ce809b250c,649059f0-5488-11ec-975c-2a01cb15032a,598f0ccf-a50d-11ec-982e-0aa1229c1b9a,f7fe8760-bc3e-11ec-b66d-2a010e0a0b16,68862530-5488-11ec-975c-2a01cb15032a,e2e145c0-515f-11ec-975c-2a010e0a0b16,1b4b931d-149f-11ed-8002-0aa1229c1b9a,36ccfe21-172b-11ed-8002-0aa202fc1b9a,f843ba4b-bc3e-11ec-b66d-2a010e0a0b16,373b7b40-52eb-11ec-8357-2a010e0a0b16,f82fe729-c7c6-11ec-981d-2a010e0a0b16,c8bc7660-515f-11ec-8357-2a010e0a0b16,81042ff0-5468-11ec-975c-2a010e0a0b16,73c62b2f-cca4-11ec-981d-200114580d00,f95bf513-bc3e-11ec-b66d-2a010e0a0b16,c6fe667e-f23b-11ec-b9c0-2a010e0a0b16,385e2950-52eb-11ec-8357-2a010e0a0b16,0ef2f88b-cca6-11ec-b790-200114580d00,0d5c2790-cca6-11ec-b790-200114580d00,e5b9bee8-166f-11ed-8002-0aa204151b9a,4bd9e0c3-ccc2-11ec-b790-2a010e0a0b16,4e8fb968-16f4-11ed-8002-0aa140651b9a,02d318c8-a46f-11ec-982e-0aa2043e1b9a,42397504-037d-11ed-8001-0aa204131b9a,b929213c-a46e-11ec-982e-0aa2043e1b9a,13bad351-1301-11ed-8002-0aa204e61b9a,e704b36a-d22d-11ec-8dd8-511cc1ec24ee,3712e4a0-52eb-11ec-8357-2a010e0a0b16,79c59697-cbc4-11ec-b790-200114580202,8c2ed1ff-ccc0-11ec-b790-2a010e0a0b16,c3df1e31-e67c-11ec-8003-0aa202a21b9a,e6e40961-d22d-11ec-8dd8-511cc1ec24ee,27c55ffd-aaa4-11ec-982e-0aa2043e1b9a,b9bd65e0-3d53-11ec-8357-2a010e0a09fb,f50a6d64-2879-11ed-8141-200114580202,28fa0575-287c-11ed-ac0b-200114580202,3c58dd90-16df-11ed-8002-0aa140641b9a,992dd7f5-16f3-11ed-8002-0aa1229c1b9a,49d9b221-16fa-11ed-8002-0aa1229c1b9a,db366855-16d7-11ed-8002-0aa202a21b9a,db2e561a-16d7-11ed-8002-0aa202a21b9a";

// THIS IS THE SSL TEST ! THIS IS THE SSL TEST ! THIS IS THE SSL TEST ! THIS IS THE SSL TEST !
o2::ccdb::CcdbApi api; 


/*
g++ -std=c++11 GigaTest.cpp -lpthread -lcurl -luv -o GigaTest && ./GigaTest
*/

// Ideas: call handleSocket for each socket to close them (mark with flag passed outside)

/*
TODO:

- add asynchronous closeLoop call

- check what can be free'd in destructor
- free things in finalize Download

- change name "checkGlobals"
- pooling threads only when they exist

- multiple uv loop threads

Information:

- Curl multi handle automatically reuses connections. Source: https://everything.curl.dev/libcurl/connectionreuse
- Keepalive for http is set to 118 seconds by default by CURL Source: https://stackoverflow.com/questions/60141625/libcurl-how-does-connection-keep-alive-work

*/

AsynchronousDownloader::AsynchronousDownloader()
{
  // Preparing timer to be used by curl
  timeout = new uv_timer_t();
  timeout->data = this;
  uv_loop_init(&loop);
  uv_timer_init(&loop, timeout);

  // Preparing curl handle
  initializeMultiHandle();  

  // Global timer
  // uv_loop runs only when there are active handles, this handle guarantees the loop won't close after finishing first batch of requests
  auto timerCheckQueueHandle = new uv_timer_t();
  timerCheckQueueHandle->data = this;
  uv_timer_init(&loop, timerCheckQueueHandle);
  uv_timer_start(timerCheckQueueHandle, checkGlobals, 100, 100);

  loopThread = new std::thread(&AsynchronousDownloader::asynchLoop, this);
}

void AsynchronousDownloader::initializeMultiHandle()
{
  curlMultiHandle = curl_multi_init();
  curl_multi_setopt(curlMultiHandle, CURLMOPT_SOCKETFUNCTION, handleSocket);
  auto socketData = new DataForSocket();
  socketData->curlm = curlMultiHandle;
  socketData->objPtr = this;
  curl_multi_setopt(curlMultiHandle, CURLMOPT_SOCKETDATA, socketData);
  curl_multi_setopt(curlMultiHandle, CURLMOPT_TIMERFUNCTION, startTimeout);
  curl_multi_setopt(curlMultiHandle, CURLMOPT_TIMERDATA, timeout);
  curl_multi_setopt(curlMultiHandle, CURLMOPT_MAX_TOTAL_CONNECTIONS, maxHandlesInUse);

}

void onUVClose(uv_handle_t* handle)
{
  if (handle != NULL)
  {
    delete handle;
  }
}

void closeHandles(uv_handle_t* handle, void* arg)
{
  if (!uv_is_closing(handle))
    uv_close(handle, onUVClose);
}

AsynchronousDownloader::~AsynchronousDownloader()
{
  // Close async thread
  closeLoop = true;
  loopThread->join();
  free(loopThread);

  // Close and if any handles are running then signal to close, and run loop once to close them
  // This may take more then one iteration of loop - hence the "while"
  while (UV_EBUSY == uv_loop_close(&loop)) {
    closeLoop = false;
    uv_walk(&loop, closeHandles, NULL);
    uv_run(&loop, UV_RUN_ONCE);
  }
  curl_multi_cleanup(curlMultiHandle);
}

bool alienRedirect(CURL* handle)
{
  char *url = NULL;
  curl_easy_getinfo(handle, CURLINFO_REDIRECT_URL, &url);
  if (url)
  {
    std::string urlStr(url);
    if (urlStr.find("alien") == std::string::npos)
      return false;
  }
  return true;
}

void closePolls(uv_handle_t* handle, void* arg)
{
  if (handle->type == UV_POLL) {
    if (!uv_is_closing(handle)) {
      uv_close(handle, onUVClose);
    }
  }
}

void onTimeout(uv_timer_t *req)
{
  auto AD = (AsynchronousDownloader *)req->data;
  int running_handles;
  curl_multi_socket_action(AD->curlMultiHandle, CURL_SOCKET_TIMEOUT, 0,
                           &running_handles);
  AD->checkMultiInfo();
}

// Is used to react to polling file descriptors in poll_handle
// Calls handle_socket indirectly for further reading*
// If call is finished closes handle indirectly by check multi info
void curl_perform(uv_poll_t *req, int status, int events)
{
  int running_handles;
  int flags = 0;
  if (events & UV_READABLE)
    flags |= CURL_CSELECT_IN;
  if (events & UV_WRITABLE)
    flags |= CURL_CSELECT_OUT;

  auto context = (AsynchronousDownloader::curl_context_t *)req->data;
  
  curl_multi_socket_action(context->objPtr->curlMultiHandle, context->sockfd, flags,
                           &running_handles);
  context->objPtr->checkMultiInfo();
}

// Initializes a handle using a socket and passes it to context
AsynchronousDownloader::curl_context_t *AsynchronousDownloader::createCurlContext(curl_socket_t sockfd, AsynchronousDownloader *objPtr)
{
  curl_context_t *context;

  context = (curl_context_t *)malloc(sizeof(*context));
  context->objPtr = objPtr;
  context->sockfd = sockfd;

  uv_poll_init_socket(&objPtr->loop, &context->poll_handle, sockfd);
  context->poll_handle.data = context;

  return context;
}

// Frees data from curl handle inside uv_handle*
void AsynchronousDownloader::curlCloseCB(uv_handle_t *handle)
{
  curl_context_t *context = (curl_context_t *)handle->data;
  free(context);
}

// Makes an asynchronious call to free curl context*
void AsynchronousDownloader::destroyCurlContext(curl_context_t *context)
{
  uv_close((uv_handle_t *)&context->poll_handle, curlCloseCB);
}

void callbackWrappingFunction(void (*cbFun)(void*), void* data, bool* completionFlag)
{
  cbFun(data);
  *completionFlag = true;
}

void AsynchronousDownloader::finalizeDownload(CURL* easy_handle)
{
  handlesInUse--;
  char* done_url;
  curl_easy_getinfo(easy_handle, CURLINFO_EFFECTIVE_URL, &done_url);
  // printf("%s DONE\n", done_url);

  PerformData *data;
  curl_easy_getinfo(easy_handle, CURLINFO_PRIVATE, &data);
  curl_multi_remove_handle(curlMultiHandle, easy_handle);

  long code;
  curl_easy_getinfo(easy_handle, CURLINFO_RESPONSE_CODE, &code);
  if (code != 303 && code != 304)
  {
    std::cout << "Weird code returned: " << code << "\n";
  }
  else
  {
    if (!alienRedirect(easy_handle))
      std::cout << "Redirected to a different server than alien\n";
  }
  // curl_easy_getinfo(easy_handle,  CURLINFO_RESPONSE_CODE, data->codeDestination);

  if (data->callback)
  {
    bool *cbFlag = (bool *)malloc(sizeof(bool));
    *cbFlag = false;
    auto cbThread = new std::thread(&callbackWrappingFunction, data->cbFun, data->cbData, cbFlag);
    threadFlagPairVector.emplace_back(cbThread, cbFlag);
  }

  // If no requests left then signal finished based on type of operation
  if (--(*data->requestsLeft) == 0)
  {
    switch (data->type)
    {
    case BLOCKING:
      data->cv->notify_all();
      break;
    case ASYNCHRONOUS:
      *data->completionFlag = true;
      break;
    case ASYNCHRONOUS_WITH_CALLBACK:
      *data->completionFlag = true;
      // TODO launch callback
      break;
    }
  }
  free(data);

  // Check if any handles are waiting in queue
  checkHandleQueue();

  // Calling timout starts a new download
  int running_handles;
  curl_multi_socket_action(curlMultiHandle, CURL_SOCKET_TIMEOUT, 0, &running_handles);
  checkMultiInfo();
}

void AsynchronousDownloader::checkMultiInfo()
{
  CURLMsg *message;
  int pending;

  while ((message = curl_multi_info_read(curlMultiHandle, &pending)))
  {
    switch (message->msg)
    {
    case CURLMSG_DONE:
    {
      /* Do not use message data after calling curl_multi_remove_handle() and
        curl_easy_cleanup(). As per curl_multi_info_read() docs:
        "WARNING: The data the returned pointer points to will not survive
        calling curl_multi_cleanup, curl_multi_remove_handle or
        curl_easy_cleanup." */
      finalizeDownload(message->easy_handle);
      
    }
    break;

    default:
      fprintf(stderr, "CURLMSG default\n");
      break;
    }
  }
}

// Connects curl timer with uv timer
int AsynchronousDownloader::startTimeout(CURLM *multi, long timeout_ms, void *userp)
{
  auto timeout = (uv_timer_t *)userp;

  if (timeout_ms < 0)
  {
    uv_timer_stop(timeout);
  }
  else
  {
    if (timeout_ms == 0)
      timeout_ms = 1; // Calling onTimout when timeout = 0 could create an infinite loop                       
    uv_timer_start(timeout, onTimeout, timeout_ms, 0);
  }
  return 0;
}

void closeSocketCallback(uv_timer_t* handle)
{
  AsynchronousDownloader::CloseSocketData* closeData = (AsynchronousDownloader::CloseSocketData*)handle->data;
  curl_socket_t socket = closeData->socket;
  auto AD = closeData->AD;

  close(socket);
  if (AD->socketTimerMap.find(socket) != AD->socketTimerMap.end()) {
    closeData->AD->socketTimerMap.erase(closeData->socket);
  } else {
    std::cout << "Error. Timer ran out for non existing socket.\n";
  }

  int running_handles;
  curl_multi_socket_action(AD->curlMultiHandle, socket, CURL_SOCKET_TIMEOUT, &running_handles);
  AD->checkMultiInfo();

  free(handle->data);
}

// Is used to react to curl_multi_socket_action
// If INOUT then assigns socket to multi handle and starts polling file descriptors in poll_handle by callback
int handleSocket(CURL *easy, curl_socket_t s, int action, void *userp,
                                         void *socketp)
{
  auto socketData = (AsynchronousDownloader::DataForSocket *)userp;
  auto AD = (AsynchronousDownloader*)socketData->objPtr;
  AsynchronousDownloader::curl_context_t *curl_context;
  int events = 0;

  switch (action)
  {
  case CURL_POLL_IN:
  case CURL_POLL_OUT:
  case CURL_POLL_INOUT:

    // Create context associated with socket and create a poll for said socket
    curl_context = socketp ? (AsynchronousDownloader::curl_context_t *)socketp : AD->createCurlContext(s, socketData->objPtr);
    curl_multi_assign(socketData->curlm, s, (void *)curl_context);

    if (action != CURL_POLL_IN)
      events |= UV_WRITABLE;
    if (action != CURL_POLL_OUT)
      events |= UV_READABLE;

    uv_poll_start(&curl_context->poll_handle, events, curl_perform);
    break;
  case CURL_POLL_REMOVE:
    if (socketp)
    {
      if (AD->socketTimerMap.find(s) == AD->socketTimerMap.end()) {
        auto timer = new uv_timer_t();
        auto closeData = (AsynchronousDownloader::CloseSocketData*)malloc(sizeof(AsynchronousDownloader::CloseSocketData));
        closeData->socket = s;
        closeData->AD = AD;
        timer->data = closeData;

        AD->socketTimerMap[s] = timer;
        uv_timer_init(&AD->loop, timer);
      }
      uv_timer_start(AD->socketTimerMap[s], closeSocketCallback, AD->socketTimoutMS, 0);

      // Stop polling the socket, remove context assiciated with it. Socket will stay open until multi handle is closed.
      uv_poll_stop(&((AsynchronousDownloader::curl_context_t *)socketp)->poll_handle);
      AD->destroyCurlContext((AsynchronousDownloader::curl_context_t *)socketp);
      curl_multi_assign(socketData->curlm, s, NULL);
    }
    break;
  default:
    abort();
  }
  return 0;
}

void AsynchronousDownloader::checkHandleQueue()
{

  // Lock access to handle queue
  handlesQueueLock.lock();
  if (handlesToBeAdded.size() > 0)
  {
    // Add handles without going over the limit
    while(handlesToBeAdded.size() > 0 && handlesInUse < maxHandlesInUse) {
      curl_multi_add_handle(curlMultiHandle, handlesToBeAdded.front());
      handlesInUse++;
      handlesToBeAdded.erase(handlesToBeAdded.begin());
    }
  }
  handlesQueueLock.unlock();
}

void checkGlobals(uv_timer_t *handle)
{
  // Check for closing signal
  auto AD = (AsynchronousDownloader*)handle->data;
  if(AD->closeLoop) {
    uv_timer_stop(handle);
  }

  // Join and erase threads that finished running callback functions
  for (int i = 0; i < AD->threadFlagPairVector.size(); i++)
  {
    if (*(AD->threadFlagPairVector[i].second))
    {
      AD->threadFlagPairVector[i].first->join();
      delete (AD->threadFlagPairVector[i].first);
      delete (AD->threadFlagPairVector[i].second);
      AD->threadFlagPairVector.erase(AD->threadFlagPairVector.begin() + i);
    }
  }
}

void AsynchronousDownloader::asynchLoop()
{
  uv_run(&loop, UV_RUN_DEFAULT);
}

std::vector<CURLcode>* AsynchronousDownloader::batchAsynchPerform(std::vector<CURL*> handleVector, bool *completionFlag)
{
  auto codeVector = new std::vector<CURLcode>(handleVector.size());
  size_t *requestsLeft = new size_t();
  *requestsLeft = handleVector.size();

  handlesQueueLock.lock();
  for(int i = 0; i < handleVector.size(); i++)
  {
    auto *data = new AsynchronousDownloader::PerformData();

    data->codeDestination = &(*codeVector)[i];
    data->requestsLeft = requestsLeft;
    data->completionFlag = completionFlag;
    data->type = ASYNCHRONOUS;

    curl_easy_setopt(handleVector[i], CURLOPT_PRIVATE, data);
    handlesToBeAdded.push_back(handleVector[i]);
  }
  handlesQueueLock.unlock();
  makeLoopCheckQueueAsync();
  return codeVector;
}

CURLcode AsynchronousDownloader::blockingPerform(CURL* handle)
{
  std::vector<CURL*> handleVector;
  handleVector.push_back(handle);
  return batchBlockingPerform(handleVector).back();
}

std::vector<CURLcode> AsynchronousDownloader::batchBlockingPerform(std::vector<CURL*> handleVector)
{
  std::condition_variable cv;
  std::mutex cv_m;
  std::unique_lock<std::mutex> lk(cv_m);

  std::vector<CURLcode> codeVector(handleVector.size());
  size_t requestsLeft = handleVector.size();

  handlesQueueLock.lock();
  for(int i = 0; i < handleVector.size(); i++)
  {
    auto *data = new AsynchronousDownloader::PerformData();
    data->codeDestination = &codeVector[i];
    data->cv = &cv;
    data->type = BLOCKING;
    data->requestsLeft = &requestsLeft;

    curl_easy_setopt(handleVector[i], CURLOPT_PRIVATE, data);
    handlesToBeAdded.push_back(handleVector[i]);
  }
  handlesQueueLock.unlock();
  makeLoopCheckQueueAsync();
  cv.wait(lk);
  return codeVector;
}

// TODO turn to batch asynch with callback
CURLcode *AsynchronousDownloader::asynchPerformWithCallback(CURL* handle, bool *completionFlag, void (*cbFun)(void*), void* cbData)
{
  auto data = new AsynchronousDownloader::PerformData();
  auto code = new CURLcode();
  data->completionFlag = completionFlag;
  data->codeDestination = code;

  data->cbFun = cbFun;
  data->cbData = cbData;
  data->callback = true;
  data->type = ASYNCHRONOUS_WITH_CALLBACK;

  curl_easy_setopt(handle, CURLOPT_PRIVATE, data);

  handlesQueueLock.lock();
  handlesToBeAdded.push_back(handle);
  handlesQueueLock.unlock();

  return code;
}

void asyncUVHandleCallback(uv_async_t *handle)
{
  auto AD = (AsynchronousDownloader*)handle->data;
  uv_close((uv_handle_t*)handle, nullptr);
  delete handle;
  // stop handle and free its memory
  AD->checkHandleQueue();
  // uv_check_t will delete be deleted in its callback
}

void AsynchronousDownloader::makeLoopCheckQueueAsync()
{
  auto asyncHandle = new uv_async_t();
  asyncHandle->data = this;
  uv_async_init(&loop, asyncHandle, asyncUVHandleCallback);
  uv_async_send(asyncHandle);
}

// ------------------------------ TESTING ------------------------------ 

std::string extractETAG(std::string headers)
{
  auto etagLine = headers.find("ETag");
  auto etagStart = headers.find("\"", etagLine)+1;
  auto etagEnd = headers.find("\"", etagStart+1);
  return headers.substr(etagStart, etagEnd - etagStart);
}

size_t writeToString(void *contents, size_t size, size_t nmemb, std::string *dst)
{
  char *conts = (char *)contents;
  for (int i = 0; i < nmemb; i++)
  {
    (*dst) += *(conts++);
  }
  return size * nmemb;
}

void cleanAllHandles(std::vector<CURL*> handles)
{
  for(auto handle : handles)
    curl_easy_cleanup(handle);
}

void closesocket_callback(void *clientp, curl_socket_t item)
{
  // std::cout << "Closing socket " << item << "\n";
  // close(item);
}

curl_socket_t opensocket_callback(void *clientp, curlsocktype purpose, struct curl_sockaddr *address)
{
  auto AD = (AsynchronousDownloader*)clientp;
  auto sock = socket(address->family, address->socktype, address->protocol);
  // AD->activeSockets.push_back(sock);  
  // std::cout << "Opening socket " << sock << "\n";
  return sock;
}

void setHandleOptions(CURL* handle, std::string* dst, std::string* headers, std::string* path, AsynchronousDownloader* AD)
{
  if (AD) {
    curl_easy_setopt(handle, CURLOPT_CLOSESOCKETFUNCTION, closesocket_callback);
    curl_easy_setopt(handle, CURLOPT_OPENSOCKETFUNCTION, opensocket_callback);
    curl_easy_setopt(handle, CURLOPT_OPENSOCKETDATA, AD);
  }

  curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, writeToString);
  curl_easy_setopt(handle, CURLOPT_HEADERDATA, headers);

  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeToString);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, dst);
  curl_easy_setopt(handle, CURLOPT_URL, path->c_str());


  api.curlSetSSLOptions(handle);
}

void setHandleOptionsForValidity(CURL* handle, std::string* dst, std::string* url, std::string* etag, AsynchronousDownloader* AD)
{

  if (AD) {
    curl_easy_setopt(handle, CURLOPT_CLOSESOCKETFUNCTION, closesocket_callback);
    curl_easy_setopt(handle, CURLOPT_OPENSOCKETFUNCTION, opensocket_callback);
    curl_easy_setopt(handle, CURLOPT_OPENSOCKETDATA, AD);
  }
  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeToString);
  curl_easy_setopt(handle, CURLOPT_WRITEDATA, dst);
  curl_easy_setopt(handle, CURLOPT_URL, url->c_str());

  api.curlSetSSLOptions(handle);

  std::string etagHeader = "If-None-Match: \"" + *etag + "\"";
  struct curl_slist *curlHeaders = nullptr;
  curlHeaders = curl_slist_append(curlHeaders, etagHeader.c_str());
  curl_easy_setopt(handle, CURLOPT_HTTPHEADER, curlHeaders);
}

std::vector<std::string> createPathsFromCS()
{
  std::vector<std::string> vec;
  std::string temp = "";
  for(int i = 0; i < pathsCS.size(); i++)
  {
    if (pathsCS[i] == ',') {
      vec.push_back(temp);
      temp = "";
    }
    else {
      (temp.push_back(pathsCS[i]));
    }
  }
  return vec;
}

std::vector<std::string*> createEtagsFromCS()
{
  std::vector<std::string*> vec;
  std::string *tmp = new std::string();
  for(int i = 0; i < etagsCS.size(); i++) {
    if (etagsCS[i] == ',') {
      vec.push_back(tmp);
      tmp = new std::string();
    } else {
      (*tmp) += etagsCS[i];
    }
  }
  vec.push_back(tmp);
  return vec;
}

int64_t blockingBatchTestSockets(int pathLimit = 0, bool printResult = false)
{
  // Preparing for downloading
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;
  
  AsynchronousDownloader AD;

  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    handles.push_back(curl_easy_init());
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handles.back(), results.back(), headers.back(), &paths[i], &AD);
  }

  // Downloading objects
  std::cout << "Starting first batch\n";
  AD.batchBlockingPerform(handles);
  cleanAllHandles(handles);
  std::cout << "First batch completed\n";


  // Waiting for sockets to close
  sleep(5);

  std::vector<CURL*> handles2;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    handles2.push_back(curl_easy_init());
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handles2.back(), results.back(), headers.back(), &paths[i], &AD);
  }

  // Downloading again
  std::cout << "Starting second batch\n";
  AD.batchBlockingPerform(handles2);
  cleanAllHandles(handles2);
  std::cout << "Second batch completed\n";

  return 0;
}

int64_t blockingBatchTest(int pathLimit = 0, bool printResult = false)
{
  // Preparing for downloading
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;
  
  AsynchronousDownloader AD;

  auto start = std::chrono::system_clock::now();
  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    handles.push_back(curl_easy_init());
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handles.back(), results.back(), headers.back(), &paths[i], &AD);
  }

  // Downloading objects
  AD.batchBlockingPerform(handles);

  cleanAllHandles(handles);
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "BLOCKING BATCH TEST:  Download - " <<  difference << "ms.\n";

  // Extracting etags
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    urlETagMap[paths[i]] = extractETAG(*headers[i]);
  }
  return difference;
}

int64_t blockingBatchTestValidity(int pathLimit = 0, bool printResult = false)
{
  // Preparing for checking objects validity
  auto paths = createPathsFromCS();
  auto etags = createEtagsFromCS();
  auto start = std::chrono::system_clock::now();

  AsynchronousDownloader AD;

  std::vector<std::string*> results;
  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    auto handle = curl_easy_init();
    results.push_back(new std::string());
    handles.push_back(handle);
    setHandleOptionsForValidity(handle, results.back(), &paths[i], etags[i], &AD);
  }

  // Checking objects validity

  AD.batchBlockingPerform(handles);

  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    long code;
    curl_easy_getinfo(handles[i], CURLINFO_RESPONSE_CODE, &code);
    if (code != 304) {
      char* url;
      curl_easy_getinfo(handles[i], CURLINFO_EFFECTIVE_URL, &url);
      std::cout << "INVALID CODE: " << code << ", URL: " << url << "\n";
    }
  }

  // Clean handles and print time
  cleanAllHandles(handles);
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "BLOCKING BATCH TEST:  Check validity - " <<  difference << "ms.\n";
  return difference;
}

int64_t asynchBatchTest(int pathLimit = 0, bool printResult = false)
{
  // Preparing urls and objects to write into
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;
  
  // Preparing downloader
  AsynchronousDownloader AD;

  auto start = std::chrono::system_clock::now();

  // Preparing handles
  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    handles.push_back(curl_easy_init());
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handles.back(), results.back(), headers.back(), &paths[i], &AD);
  }

  // Performing requests
  bool requestFinished = false;
  AD.batchAsynchPerform(handles, &requestFinished);
  while (!requestFinished) sleep(0.05);

  // Cleanup and print time
  cleanAllHandles(handles);
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "ASYNC BATCH TEST:     download - " << difference << "ms.\n";

  // Extracting etags
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    urlETagMap[paths[i]] = extractETAG(*headers[i]);
  }
  return difference;
}

int64_t asynchBatchTestValidity(int pathLimit = 0, bool printResult = false)
{
  // Preparing for checking objects validity
  auto paths = createPathsFromCS();
  auto etags = createEtagsFromCS();
  auto start = std::chrono::system_clock::now();
  AsynchronousDownloader AD;

  std::vector<std::string*> results;
  std::vector<CURL*> handles;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    auto handle = curl_easy_init();
    results.push_back(new std::string());
    handles.push_back(handle);
    setHandleOptionsForValidity(handle, results.back(), &paths[i], etags[i], &AD);
  }

  // Checking objects validity
  bool requestFinished = false;
  AD.batchAsynchPerform(handles, &requestFinished);
  while (!requestFinished) sleep(0.001);
  cleanAllHandles(handles);

  // Checking if objects did not change
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    long code;
    curl_easy_getinfo(handles[i], CURLINFO_RESPONSE_CODE, &code);
    if (code != 304) {
      std::cout << "INVALID CODE: " << code << "\n";
    }
  }

  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "ASYNC BATCH TEST:     Check validity - " <<  difference << "ms.\n";
  return difference;
}

int64_t linearTest(int pathLimit = 0, bool printResult = false)
{
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;

  auto start = std::chrono::system_clock::now();
  CURL *handle = curl_easy_init();
  curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, writeToString);

  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handle, results.back(), headers.back(), &paths[i], nullptr);
    curl_easy_perform(handle);

    long code;
    curl_easy_getinfo(handle, CURLINFO_HTTP_CODE, &code);
    if (code != 303) 
      std::cout << "Result different that 303. Received code: " << code << "\n";
  }

  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

  curl_easy_cleanup(handle);
  
  // Extracting etags
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    urlETagMap[paths[i]] = extractETAG(*headers[i]);
  }
  if (printResult)
    std::cout << "LINEAR TEST:          download - " << difference << "ms.\n";
  return difference;
}

int64_t linearTestValidity(int pathLimit = 0, bool printResult = false)
{
  // Preparing for checking objects validity
  auto paths = createPathsFromCS();
  auto etags = createEtagsFromCS();
  auto start = std::chrono::system_clock::now();

  std::vector<std::string*> results;
  CURL* handle = curl_easy_init();
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    results.push_back(new std::string());    
    setHandleOptionsForValidity(handle, results.back(), &paths[i], etags[i], nullptr);
    
    curl_easy_perform(handle);
    long code;
    curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &code);
    if (code != 304) {
      std::cout << "INVALID CODE: " << code << "\n";
    }
  }

  curl_easy_cleanup(handle);
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "LINEAR TEST:          Check validity - " <<  difference << "ms.\n";
  return difference;
}

int64_t linearTestNoReuse(int pathLimit = 0, bool printResult = false)
{
  auto paths = createPathsFromCS();
  std::vector<std::string*> results;
  std::vector<std::string*> headers;
  std::unordered_map<std::string, std::string> urlETagMap;

  auto start = std::chrono::system_clock::now();

  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    CURL* handle = curl_easy_init();
    results.push_back(new std::string());
    headers.push_back(new std::string());
    setHandleOptions(handle, results.back(), headers.back(), &paths[i], nullptr);
    curl_easy_perform(handle);

    long code;
    curl_easy_getinfo(handle, CURLINFO_HTTP_CODE, &code);
    if (code != 303) 
      std::cout << "Result different that 303. Received code: " << code << "\n";

    curl_easy_cleanup(handle);
  }
  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "LINEAR TEST no reuse:      download - " <<  difference << "ms.\n";

  // Extracting etags
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    urlETagMap[paths[i]] = extractETAG(*headers[i]);
  }
  return difference;
}

int64_t linearTestNoReuseValidity(int pathLimit = 0, bool printResult = false)
{
  // Preparing for checking objects validity
  auto paths = createPathsFromCS();
  auto etags = createEtagsFromCS();
  auto start = std::chrono::system_clock::now();

  std::vector<std::string*> results;
  for (int i = 0; i < (pathLimit == 0 ? paths.size() : pathLimit); i++) {
    CURL* handle = curl_easy_init();
    results.push_back(new std::string());
    setHandleOptionsForValidity(handle, results.back(), &paths[i], etags[i], nullptr);
    
    curl_easy_perform(handle);
    long code;
    curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &code);
    if (code != 304) {
      std::cout << "INVALID CODE: " << code << "\n";
    }
    curl_easy_cleanup(handle);
  }

  auto end = std::chrono::system_clock::now();
  auto difference = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
  if (printResult)
    std::cout << "LINEAR NO REUSE TEST: Check validity - " <<  difference << "ms.\n";
  return difference;
}

int64_t countAverageTime(int64_t (*function)(int, bool), int arg, int repeats)
{
  int64_t sum = 0;
  for(int i = 0; i < repeats; i++) {
    sum += function(arg, false);
  }
  return sum / repeats;
}

// int main()
void GigaTest()
{
  api.init("https://alice-ccdb.cern.ch");

  if (curl_global_init(CURL_GLOBAL_ALL))
  {
    fprintf(stderr, "Could not init curl\n");
    return;
  }

  int testSize = 100; // max 185

  if (testSize != 0)
    std::cout << "-------------- Testing for " << testSize << " objects with " << AsynchronousDownloader::maxHandlesInUse << " parallel connections. -----------\n";
  else
    std::cout << "-------------- Testing for all objects with " << AsynchronousDownloader::maxHandlesInUse << " parallel connections. -----------\n";


  int repeats = 10;


  // Just checking for 303
  // std::cout << "Blocking perform: " << countAverageTime(blockingBatchTest, testSize, repeats) << "ms.\n";
  // std::cout << "Async    perform: " << countAverageTime(asynchBatchTest, testSize, repeats) << "ms.\n";
  // std::cout << "Single   handle : " << countAverageTime(linearTest, testSize, repeats) << "ms.\n";
  // std::cout << "Signle no reuse : " << countAverageTime(linearTestNoReuse, testSize, repeats) << "ms.\n";

  // blockingBatchTestValidity(testSize);
  // asynchBatchTestValidity(testSize);
  // linearTestValidity(testSize);
  // linearTestNoReuseValidity(testSize);

  // std::cout << "--------------------------------------------------------------------------------------------\n";

  std::cout << "Blocking perform validity: " << countAverageTime(blockingBatchTestValidity, testSize, repeats) << "ms.\n";
  std::cout << "Async    perform validity: " << countAverageTime(asynchBatchTestValidity, testSize, repeats) << "ms.\n";
  std::cout << "Single   handle  validity: " << countAverageTime(linearTestValidity, testSize, repeats) << "ms.\n";
  std::cout << "Single no reuse  validity: " << countAverageTime(linearTestNoReuseValidity, testSize, repeats) << "ms.\n";

  // blockingBatchTestSockets(testSize, false);

  curl_global_cleanup();
  return;
}