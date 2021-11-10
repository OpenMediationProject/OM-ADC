// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.adc.web;

import com.adtiming.om.adc.dto.Response;
import com.adtiming.om.adc.service.adcolony.RequestAdcolony;
import com.adtiming.om.adc.service.admob.RequestAdmob;
import com.adtiming.om.adc.service.adtiming.RequestAdtiming;
import com.adtiming.om.adc.service.applovin.RequestApplovin;
import com.adtiming.om.adc.service.chartboost.RequestChartboost;
import com.adtiming.om.adc.service.facebook.RequestFacebook;
import com.adtiming.om.adc.service.helium.RequestHelium;
import com.adtiming.om.adc.service.ironsource.RequestIronSource;
import com.adtiming.om.adc.service.kuaishou.RequestKuaiShou;
import com.adtiming.om.adc.service.mint.RequestMint;
import com.adtiming.om.adc.service.mintegral.RequestMintegral;
import com.adtiming.om.adc.service.mopub.RequestMopub;
import com.adtiming.om.adc.service.pubnative.RequestPubNative;
import com.adtiming.om.adc.service.shareit.RequestSharEit;
import com.adtiming.om.adc.service.sigmob.RequestSigmob;
import com.adtiming.om.adc.service.tapjoy.RquestTapjoy;
import com.adtiming.om.adc.service.tencent.RequestTencent;
import com.adtiming.om.adc.service.tiktok.RequestTikTok;
import com.adtiming.om.adc.service.unity.RequestUnity;
import com.adtiming.om.adc.service.vungle.RequestVungle;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

@RestController
public class APIController extends BaseController {

    @Resource
    private RequestAdtiming adtiming;

    @Resource
    private RequestAdmob admob;

    @Resource
    private RequestFacebook facebook;

    @Resource
    private RequestUnity unity;

    @Resource
    private RequestVungle vungle;

    @Resource
    private RequestTencent tencent;

    @Resource
    private RequestAdcolony adcolony;

    @Resource
    private RequestApplovin applovin;

    @Resource
    private RequestMopub mopub;

    @Resource
    private RquestTapjoy tapjoy;

    @Resource
    private RequestChartboost chartboost;

    @Resource
    private RequestTikTok tikTok;

    @Resource
    private RequestMintegral mintegral;

    @Resource
    private RequestIronSource ironSource;

    @Resource
    private RequestMint mint;

    @Resource
    private RequestHelium helium;

    @Resource
    private RequestKuaiShou kuaiShou;

    @Resource
    private RequestSigmob sigmob;

    @Resource
    private RequestPubNative pubNative;

    @Resource
    private RequestSharEit sharEit;

    @RequestMapping("/test")
    public Object test() {
        return Response.build();
    }

    @RequestMapping("/adtiming/rebuild")
    public Object rebuildAdtiming(String[] day, Integer id) {
        if (id != null && id > 0) {
            adtiming.rebuild(day, id);
        } else {
            adtiming.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/admob/rebuild")
    public Object rebuildAddmob(String[] day, Integer id) {
        if (id != null && id > 0) {
            admob.rebuild(day, id);
        } else  {
            admob.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/unity/rebuild")
    public Object rebuild(String[] day, int hour, Integer id) {
        if (id != null && id > 0) {
            unity.rebuild(day, hour, id, 0);
        } else {
            unity.rebuild(day, hour, 0);
        }
        return Response.build();
    }

    @RequestMapping("/unity/rebuild/day")
    public Object rebuildByDay(String[] day, Integer id) {
        if (id != null && id > 0) {
            unity.rebuild(day, 0, id, 1);
        } else {
            unity.rebuild(day, 0, 1);
        }
        return Response.build();
    }

    @RequestMapping("/facebook/rebuild")
    public Object rebuildFB(String[] day, Integer id) {
        if (id != null && id > 0) {
            facebook.rebuildTask(day, id);
        } else {
            facebook.rebuildTask(day);
        }
        return Response.build();
    }

    @RequestMapping("/vungle/rebuild")
    public Object rebuildVungle(String[] day, Integer id) {
        if (id != null && id > 0) {
            vungle.rebuild(day, id);
        } else {
            vungle.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/tencent/rebuild")
    public Object rebuildTencent(String[] day, Integer id) {
        if (id != null && id > 0) {
            tencent.rebuild(day, id);
        } else {
            tencent.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/adcolony/rebuild")
    public Object rebuildAdcolony(String[] day, Integer id) {
        if (id != null && id > 0) {
            adcolony.rebuild(day, id);
        } else {
            adcolony.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/applovin/rebuild")
    public Object rebuildApplovin(String[] day, Integer id) {
        if (id != null && id > 0) {
            applovin.rebuild(day, id);
        } else {
            applovin.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/mopub/rebuild")
    public Object rebuildMopub(String[] day, Integer id) {
        if (id != null && id >0) {
            mopub.rebuild(day, id);
        } else {
            mopub.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/tapjoy/rebuild")
    public Object rebuildTapjoy(String[] day, Integer id) {
        if (id != null && id >0) {
            tapjoy.rebuild(day, id);
        } else {
            tapjoy.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/chartboost/rebuild")
    public Object rebuildChartboost(String[] day, Integer id) {
        if (id != null && id >0) {
            chartboost.rebuild(day, id);
        } else {
            chartboost.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/tiktok/rebuild")
    public Object rebuildTikTok(String[] day, Integer id) {
        if (id != null && id >0) {
            tikTok.rebuild(day, id);
        } else {
            tikTok.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/mintegral/rebuild")
    public Object rebuildMintegral(String[] day, Integer id) {
        if (id != null && id >0) {
            mintegral.rebuild(day, id);
        } else {
            mintegral.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/ironsource/rebuild")
    public Object rebuildIronSourcce(String[] day, Integer id) {
        if (id != null) {
            ironSource.rebuild(day, id);
        } else {
            ironSource.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/mint/rebuild")
    public Object rebuildMint(String[] day, Integer id) {
        if (id != null && id >0) {
            mint.rebuild(day, id);
        } else {
            mint.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/helium/rebuild")
    public Object rebuildHelium(String[] day, Integer id) {
        if (id != null && id >0) {
            helium.rebuild(day, id);
        } else {
            helium.rebuild(day);
        }
        return Response.build();
    }

    @RequestMapping("/adnetwork/rebuild")
    public Object rebuildAdNetwrokTask(int adnId, String[] day, Integer id) {
        try {
            switch (adnId) {
                case 1:
                    if (id != null && id > 0) {
                        adtiming.rebuild(day, id);
                    } else {
                        adtiming.rebuild(day);
                    }
                    break;
                case 2:
                    if (id != null && id > 0) {
                        admob.rebuild(day, id);
                    } else {
                        admob.rebuild(day);
                    }
                    break;
                case 3:
                    if (id != null && id > 0) {
                        facebook.rebuildTask(day, id);
                    } else {
                        facebook.rebuildTask(day);
                    }
                    break;
                case 4:
                    if (id != null) {
                        unity.rebuild(day, 0, id, 1);
                    } else {
                        unity.rebuild(day, 0, 1);
                    }
                    break;
                case 5:
                    if (id != null && id > 0) {
                        vungle.rebuild(day, id);
                    } else {
                        vungle.rebuild(day);
                    }
                    break;
                case 6:
                    if (id != null && id > 0) {
                        tencent.rebuild(day, id);
                    } else {
                        tencent.rebuild(day);
                    }
                    break;
                case 7:
                    if (id != null && id > 0) {
                        adcolony.rebuild(day, id);
                    } else {
                        adcolony.rebuild(day);
                    }
                    break;
                case 8:
                    if (id != null && id > 0) {
                        applovin.rebuild(day, id);
                    } else {
                        applovin.rebuild(day);
                    }
                    break;
                case 9:
                    if (id != null && id > 0) {
                        mopub.rebuild(day, id);
                    } else {
                        mopub.rebuild(day);
                    }
                    break;
                case 11:
                    if (id != null && id > 0) {
                        tapjoy.rebuild(day, id);
                    } else {
                        tapjoy.rebuild(day);
                    }
                    break;
                case 12:
                    if (id != null && id > 0) {
                        chartboost.rebuild(day, id);
                    } else {
                        chartboost.rebuild(day);
                    }
                    break;
                case 13:
                    if (id != null && id > 0) {
                        tikTok.rebuild(day, id);
                    } else {
                        tikTok.rebuild(day);
                    }
                    break;
                case 14:
                    if (id != null && id > 0) {
                        mintegral.rebuild(day, id);
                    } else {
                        mintegral.rebuild(day);
                    }
                    break;
                case 15:
                    if (id != null && id > 0) {
                        ironSource.rebuild(day, id);
                    } else {
                        ironSource.rebuild(day);
                    }
                    break;
                case 17:
                    if (id != null && id > 0) {
                        helium.rebuild(day, id);
                    } else {
                        helium.rebuild(day);
                    }
                    break;
                case 18:
                    if (id != null && id >0) {
                        mint.rebuild(day, id);
                    } else {
                        mint.rebuild(day);
                    }
                    break;
                case 20:
                    if (id != null && id >0) {
                        sigmob.rebuild(day, id);
                    } else {
                        sigmob.rebuild(day);
                    }
                    break;
                case 21:
                    if (id != null && id >0) {
                        kuaiShou.rebuild(day, id);
                    } else {
                        kuaiShou.rebuild(day);
                    }
                    break;
                case 23:
                    if (id != null && id >0) {
                        pubNative.rebuild(day, id);
                    } else {
                        pubNative.rebuild(day);
                    }
                case 27:
                    if (id != null && id >0) {
                        sharEit.rebuild(day, id);
                    } else {
                        sharEit.rebuild(day);
                    }
                    break;
                default:
                    break;
            }
        } catch (Exception e) {
            return Response.build().code(500).msg(e.getMessage());
        }
        return Response.build();
    }
}
