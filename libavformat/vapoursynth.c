/*
 * This file is part of FFmpeg.
 *
 * FFmpeg is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * FFmpeg is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with FFmpeg; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 */

/**
* @file
* VapourSynth demuxer
*
* It is a framework to create video (and audio) scripts similar to AviSynth.
* The scripts are executed using Python without a sandbox, so those scripts
* could do dangerous stuff. As such, it needs to be specified explicitly,
* since probing is disabled for this demuxer.
*
* May synthesize vapour (?).
*
* @link https://www.vapoursynth.com Official VapourSynth website @endlink
* @link https://www.vapoursynth.com/doc/api/vapoursynth4.h.html VapourSynth API Documentation @endlink
* @link https://www.vapoursynth.com/doc/api/vsscript4.h.html VapourSynth Script API Documentation @endlink
*/

#include <limits.h>

#include <VapourSynth4.h>
#include <VSScript4.h>

#include "libavutil/avassert.h"
#include "libavutil/avstring.h"
#include "libavutil/eval.h"
#include "libavutil/frame.h"
#include "libavutil/imgutils.h"
#include "libavutil/mem.h"
#include "libavutil/opt.h"
#include "libavutil/pixdesc.h"
#include "avformat.h"
#include "demux.h"
#include "internal.h"

/**
 * @def TARGET_VAPOURSYNTH_API_VERSION
 * The API version to use for the VapourSynth API.
 */

/**
 * @def TARGET_VSSCRIPT_API_VERSION
 * The API version to use for the VapourSynth Script API.
 */

#if 1
#define TARGET_VAPOURSYNTH_API_VERSION VS_MAKE_VERSION(4, 0)
#define TARGET_VSSCRIPT_API_VERSION VS_MAKE_VERSION(4, 0)
#else // Quickly switch to the current version
#define TARGET_VAPOURSYNTH_API_VERSION VAPOURSYNTH_API_VERSION
#define TARGET_VSSCRIPT_API_VERSION VSSCRIPT_API_VERSION
#endif

/**
 * Some state that needs to kept alive when VapourSynth is still in use
 * outside of this demuxer.
 *
 * This is like this because there because the demuxer could
 * be freed while it is still in use by the video @ref AVFrame this demuxer
 * produces, which keep references to VapourSynth memory that should be kept
 * alive in this edge case.
 */
struct VSState {
    /**
     * The context object of VapourSynth itself.
     */
    VSCore *core;
    /**
     * The VapourSynth Script context object.
     */
    VSScript *script;
    /**
     * The API of VapourSynth itself.
     */
    const VSAPI *api;
    /**
     * The API of VapourSynth Script itself.
     */
    const VSSCRIPTAPI *scriptapi;
};

/**
 * Context structure for a given VapourSynth output that is managed.
 */
struct VSOutput {
    /** Index of the output channel on the VapourSynth Script side. */
    int output_index;
    /** Index of the @ref AVStream we expose. */
    int stream_index;
    /** Type of the node (audio or frame). */
    int type;
    /** Current frame to read next time in @ref read_packet_video
     * or @ref read_packet_audio. */
    int current_frame;
    /**
     * Whenever this stream has constant frame or sample rate.
     * Used to determine whenever seeking is supported. */
    int is_constant_rate;
    /** The node itself from VapourSynth. */
    VSNode *node;
    /**
     * Reorder video channels. The index is the input channel (from VapourSynth)
     * and the value is the output channel. */
    int c_order[AV_NUM_DATA_POINTERS];
};

/**
 * The main context of this demuxer.
 */
typedef struct VSContext {
    const AVClass *class;

    /**
     * Contains some state variables of VapourSynth to keep track,
     * even if this demuxer is freed but not its frames.
     *
     * This contains a @ref VSState structure.
     */
    AVBufferRef *vss_state;

    /**
     * The API of VapourSynth itself.
     */
    const VSAPI *vsapi;

    /**
     * The API of VapourSynth Script itself.
     */
    const VSSCRIPTAPI *vsscriptapi;

    /**
     * The log handle for VapourSynth when registering our log handler.
     * Used to remove it when this demuxer is getting freed.
     * @see VSContext.vs_handle_log
     */
    VSLogHandle *vslog;

    /**
     * Number of outputs of the @ref VSContext.outputs array.
     */
    unsigned int num_outputs;
    /**
     * The outputs this demuxer knows and manages.
     * The length of the array is stored in @ref VSContext.num_outputs;
     */
    struct VSOutput *outputs;
    /**
     * The current output from @ref VSContext.outputs to process when reading
     * packets.
     * @see read_packet_vs
     */
    unsigned int current_output;

    /* Options */
    /**
     * The script buffer size in bytes to reserve for the script.
     * If the script is bigger than this value, then it is truncated and a
     * warning is issued.
     */
    int64_t max_script_size;
    /**
     * Option: This demuxer attempts to query for outputs from <code>0</code>
     * to this value (exclusive) whenever it actually contains an output to
     * stream.
     */
    int64_t output_max_probe;
    /**
     * Option: Maximum cache size in bytes VapourSynth should use.
     * If <code<0</code>, use the default value from VapourSynth.
     */
    int64_t vs_cache_size;
    /**
     * Option: Number of threads VapourSynth should use.
     * If <code>0</code>, use the default value from VapourSynth.
     */
    int64_t vs_thread_count;
    /**
     * Option: A boolean indicating when it should attach the VapourSynth
     * logging facility to the libavutil
     */
    int64_t vs_handle_log;
} VSContext;

static const AVOption options[] = {
    {
        .name = "max_script_size",
        .help = "Set max script file size supported",
        .offset = offsetof(VSContext, max_script_size),
        .type = AV_OPT_TYPE_INT64,
        .default_val = {.i64 = 16 * 1024 * 1024},
        .min = 0,
        .max = SIZE_MAX - 1,
        .flags = AV_OPT_FLAG_DECODING_PARAM,
        .unit = "bytes"
    },
    {
        .name = "output_max_probe",
        .help = "Outputs to probe for discovery",
        .offset = offsetof(VSContext, output_max_probe),
        .type = AV_OPT_TYPE_INT64,
        .default_val = {.i64 = 64},
        .min = 1,
        .max = INT_MAX - 1,
        .flags = AV_OPT_FLAG_DECODING_PARAM,
    },
    {
        .name = "vs_cache_size",
        .help = "Max Framebuffer Cache Size for VS",
        .offset = offsetof(VSContext, vs_cache_size),
        .type = AV_OPT_TYPE_INT64,
        .default_val = {.i64 = 0},
        .min = 0,
        .max = INT64_MAX - 1,
        .flags = AV_OPT_FLAG_DECODING_PARAM,
        .unit = "bytes"
    },
    {
        .name = "vs_thread_count",
        .help = "Thread Count for VS",
        .offset = offsetof(VSContext, vs_thread_count),
        .type = AV_OPT_TYPE_INT64,
        .default_val = {.i64 = 0},
        .min = 0,
        .max = INT_MAX - 1,
        .unit = "threads"
    },
    {
        .name = "vs_handle_log",
        .help = "Handle logs from VS",
        .offset = offsetof(VSContext, vs_handle_log),
        .type = AV_OPT_TYPE_BOOL,
        .default_val = {.i64 = 0},
        .min = 0,
        .max = 1,
        .flags = AV_OPT_FLAG_DECODING_PARAM,
    },
    {NULL}
};

/**
 * The log handler callback that VapourSynth itself calls.
 * It forwards to libavutil own logging facility.
 *
 * @param msg_type The type of message (log level).
 * @param msg The message itself.
 * @param userdata Some context data, here the @ref AVFormatContext to log into.
 */
static void vs_log_handler(int msg_type, const char *msg, void *userdata)
{
    AVFormatContext *s = userdata;
    int level = AV_LOG_VERBOSE;
    if (msg_type == mtDebug)
        level = AV_LOG_DEBUG;
    else if (msg_type == mtInformation)
        level = AV_LOG_INFO;
    else if (msg_type == mtWarning)
        level = AV_LOG_WARNING;
    else if (msg_type == mtCritical)
        level = AV_LOG_ERROR;
    else if (msg_type == mtFatal)
        level = AV_LOG_FATAL;
    av_log(s, level, "[VS] %s\n", msg ? msg : "(unknown)");
}

/**
 * The log handler callback that VapourSynth itself calls when removing our
 * log handler.
 *
 * @param userdata Some context data, here the @ref AVFormatContext to get rid
 *                 of our reference to the log handler.
 */
static void vs_log_free(void *userdata)
{
    AVFormatContext *s = userdata;
    VSContext *vs = s->priv_data;
    vs->vslog = NULL;
}

/**
 * Callback for @ref AVBuffer to free an @ref VSState structure.
 */
static void free_vss_state(void *opaque, uint8_t *data)
{
    struct VSState *vss = opaque;

    if (vss->script && vss->scriptapi) {
        // Also frees VSCore
        vss->scriptapi->freeScript(vss->script);
        vss->script = NULL;
    } else if (vss->core && vss->api) {
        vss->api->freeCore(vss->core);
    }
    vss->core = NULL;
}

/**
 * Callback to close this demuxer instance.
 */
static av_cold int read_close_vs(AVFormatContext *s)
{
    VSContext *vs = s->priv_data;
    const struct VSState *vss_state = (const struct VSState*)vs->vss_state->data;
    int err = 0;
    int i;

    if (vs->outputs) {
        for (i = 0; i < vs->num_outputs; i++) {
            if (vs->outputs[i].node)
                vs->vsapi->freeNode(vs->outputs[i].node);
        }
        vs->num_outputs = 0;
        av_realloc(vs->outputs, 0);
        vs->outputs = NULL;
        vs->current_output = 0;
    }


    if (vs->vslog && vss_state && vss_state->core && vs->vsapi)
        vs->vsapi->removeLogHandler(vs->vslog, vss_state->core);
    else if (vs->vslog && !vss_state) {
        av_log(s, AV_LOG_ERROR, "Log handler registered, but no access to the core,"
                " this is an internal bug.\n");
        err = AVERROR_BUG;
    }

    vs->vsapi = NULL;
    vs->vsscriptapi = NULL;
    vs->vslog = NULL;

    av_buffer_unref(&vs->vss_state);

    return err;
}

/**
 * Returns whenever the sepcified pixel format is in the native endian of
 * the machine it is currently running on.
 *
 * @param pixfmt The pixel format to check.
 * @returns <code>0</code> whenever it is not in the native endian, or
 *          an error occured, or <code>1</code> whenever it doesn't matter
 *          (single-byte format or @ref AV_PIX_FMT_NONE) or is indeed in the
 *          native endian.
 */
static av_cold int is_native_endian(enum AVPixelFormat pixfmt)
{
    enum AVPixelFormat other = av_pix_fmt_swap_endianness(pixfmt);
    const AVPixFmtDescriptor *pd;
    if (other == AV_PIX_FMT_NONE || other == pixfmt)
        return 1; // not affected by byte order
    pd = av_pix_fmt_desc_get(pixfmt);
    return pd && (!!HAVE_BIGENDIAN == !!(pd->flags & AV_PIX_FMT_FLAG_BE));
}

/**
 * Try to find a matching supported pixel format from a VapourSynth pixel format.
 *
 * @param vsf The video format info VapourSynth provides to match.
 * @param[out] c_order A map from VapourSynth input video channel (index) to the
 *                     target output video channel (or plane).
 * @return The matched pixel format, or @ref AV_PIX_FMT_NONE if none found.
 */
static av_cold enum AVPixelFormat match_pixfmt(const VSVideoFormat *vsf, int c_order[AV_NUM_DATA_POINTERS])
{
    static const int yuv_order[AV_NUM_DATA_POINTERS] = {0, 1, 2, 0};
    static const int rgb_order[AV_NUM_DATA_POINTERS] = {1, 2, 0, 0};
    const AVPixFmtDescriptor *pd;

    for (pd = av_pix_fmt_desc_next(NULL); pd; pd = av_pix_fmt_desc_next(pd)) {
        int is_rgb, is_yuv, i;
        const int *order;
        enum AVPixelFormat pixfmt;

        pixfmt = av_pix_fmt_desc_get_id(pd);

        if (pd->flags & (AV_PIX_FMT_FLAG_BAYER | AV_PIX_FMT_FLAG_ALPHA |
                         AV_PIX_FMT_FLAG_HWACCEL | AV_PIX_FMT_FLAG_BITSTREAM |
                         AV_PIX_FMT_FLAG_XYZ))
            continue;

        if (pd->log2_chroma_w != vsf->subSamplingW ||
            pd->log2_chroma_h != vsf->subSamplingH)
            continue;

        is_rgb = vsf->colorFamily == cfRGB;
        if (is_rgb != !!(pd->flags & AV_PIX_FMT_FLAG_RGB))
            continue;

        is_yuv = vsf->colorFamily == cfYUV ||
                 vsf->colorFamily == cfGray;
        if (!is_rgb && !is_yuv)
            continue;

        if (vsf->sampleType != ((pd->flags & AV_PIX_FMT_FLAG_FLOAT) ? stFloat : stInteger))
            continue;

        if (av_pix_fmt_count_planes(pixfmt) != vsf->numPlanes)
            continue;

        if (!is_native_endian(pixfmt))
            continue;

        order = is_yuv ? yuv_order : rgb_order;

        for (i = 0; i < pd->nb_components; i++) {
            const AVComponentDescriptor *c = &pd->comp[i];
            if (order[c->plane] != i ||
                c->offset != 0 || c->shift != 0 ||
                c->step != vsf->bytesPerSample ||
                c->depth != vsf->bitsPerSample)
                continue;
        }

        // Use it.
        memcpy(c_order, order, sizeof(int[AV_NUM_DATA_POINTERS]));
        return pixfmt;
    }

    return AV_PIX_FMT_NONE;
}

/**
 * Returns an PCM codec ID for the specified sample format.
 * Since VapourSynth uses a planar format, only planar formats are supported.
 * However, not all PCM variants have a planar variants, so those will
 * return the non-planar format.
 *
 * This is an alternative to @ref av_get_pcm_codec, except that this function
 * prefers planar codecs when available (rather just ignoring planar samples).
 *
 * @param fmt The sample format. It must be one of a planar format
 *            (ending with <code>P</code>).
 * @return The PCM codec ID for the specified sample type (planar preferred),
 *         or @ref AV_CODEC_ID_NONE if not matching.
 * @note This function should be updated manually whenever new planar
 *       PCM codecs are added.
 */
static enum AVCodecID get_pcm_codec(enum AVSampleFormat fmt)
{
    static const enum AVCodecID map[][2] = {
        [AV_SAMPLE_FMT_S16P] = { AV_CODEC_ID_PCM_S16LE_PLANAR, AV_CODEC_ID_PCM_S16BE_PLANAR },
        [AV_SAMPLE_FMT_S32P] = { AV_CODEC_ID_PCM_S32LE_PLANAR, AV_CODEC_ID_PCM_S32BE },
        [AV_SAMPLE_FMT_S64P] = { AV_CODEC_ID_PCM_S64LE, AV_CODEC_ID_PCM_S64BE },
        [AV_SAMPLE_FMT_FLTP] = { AV_CODEC_ID_PCM_F32LE, AV_CODEC_ID_PCM_F32BE },
        [AV_SAMPLE_FMT_DBLP] = { AV_CODEC_ID_PCM_F64LE, AV_CODEC_ID_PCM_F64BE },
    };
    if (fmt < 0 || fmt >= FF_ARRAY_ELEMS(map))
        return AV_CODEC_ID_NONE;
    return map[fmt][AV_NE(1, 0)];
}

/**
 * Returns whenever the specified codec id is a PCM planar codec or not.
 *
 * @param id The codec id to check whenever it is planar or not.
 * @return <code>0</code> if the specified codec id is not planar,
 *         <code>1</code> if the specified codec is indeed planar.
 * @note This function should be updated manually whenever new planar
 *       PCM codecs are added.
 */
static int is_pcm_codec_planar(enum AVCodecID id)
{
    return id == AV_CODEC_ID_PCM_S8_PLANAR
        || id == AV_CODEC_ID_PCM_S16LE_PLANAR
        || id == AV_CODEC_ID_PCM_S16BE_PLANAR
        || id == AV_CODEC_ID_PCM_S24LE_PLANAR
        || id == AV_CODEC_ID_PCM_S32LE_PLANAR;
}

static av_cold int read_header_vs(AVFormatContext *s)
{
    AVStream *st;
    AVIOContext *pb = s->pb;
    VSContext *vs = s->priv_data;
    int64_t sz = avio_size(pb);
    char *buf = NULL;
    char dummy;
    const VSVideoInfo *vinfo;
    const VSAudioInfo *ainfo;
    struct VSCoreInfo core_info;
    struct VSState *vss_state;
    int err = 0;
    int iout;
    struct VSOutput *currout;
    char tempbuf[32]; /* getVideoFormatName and getAudioFormatName limit */

    vss_state = av_mallocz(sizeof(*vss_state));
    if (!vss_state) {
        err = AVERROR(ENOMEM);
        goto done;
    }

    vs->vss_state = av_buffer_create(NULL, 0, free_vss_state, vss_state, 0);
    if (!vs->vss_state) {
        err = AVERROR(ENOMEM);
        av_free(vss_state);
        goto done;
    }

    vs->vsscriptapi = vss_state->scriptapi = getVSScriptAPI(TARGET_VSSCRIPT_API_VERSION);
    if (!vs->vsscriptapi) {
        av_log(s, AV_LOG_ERROR, "Failed to initialize VSScript API"
                " (possibly PYTHONPATH not set,"
                " or you have an outdated version, requires VSSCRPITAPI 0x%0x).\n",
                TARGET_VSSCRIPT_API_VERSION);
        err = AVERROR_EXTERNAL;
        goto done;
    }
    av_log(s, AV_LOG_VERBOSE, "VSSCRIPTAPI version: 0x%x (compiled: 0x%x)\n",
            vs->vsscriptapi->getAPIVersion(), TARGET_VSSCRIPT_API_VERSION);

    vs->vsapi = vss_state->api = vs->vsscriptapi->getVSAPI(TARGET_VAPOURSYNTH_API_VERSION);
    if (!vs->vsapi) {
        av_log(s, AV_LOG_ERROR, "Failed to initialize VS API"
                " (possibly PYTHONPATH not set,"
                " or you have an outdated version, requires VSAPI 0x%0x).\n",
                TARGET_VAPOURSYNTH_API_VERSION);
        err = AVERROR_EXTERNAL;
        goto done;
    }
    av_log(s, AV_LOG_VERBOSE, "VSAPI version: 0x%x (compiled: 0x%x)\n",
            vs->vsapi->getAPIVersion(), VAPOURSYNTH_API_VERSION);

    vss_state->script = vs->vsscriptapi->createScript(NULL);
    if (!vss_state->vss) {
        av_log(s, AV_LOG_ERROR, "Failed to create script instance.\n");
        err = AVERROR_EXTERNAL;
        goto done;
    }

    vss_state->core = vs->vsscriptapi->getCore(vss_state->vss);
    if (!vss_state->core) {
        av_log(s, AV_LOG_ERROR, "Failed to get core instance.\n");
        err = AVERROR_EXTERNAL;
        goto done;
    }

    if (vs->vs_handle_log) {
        vs->vslog = vs->vsapi->addLogHandler(vs_log_handler, vs_log_free, s, vss_state->core);
        if (!vs->vslog) {
            av_log(s, AV_LOG_WARNING,
                    "Failed to register as a log handler, no logs from VS may be delivered.");
        }
    }

    if (vs->vs_cache_size != 0)
        vs->vsapi->setMaxCacheSize(vs->vs_cache_size, vss_state->core);
    if (vs->vs_thread_count != 0)
        vs->vsapi->setThreadCount(vs->vs_thread_count, vss_state->core);
    vs->vsapi->getCoreInfo(vss_state->core, &core_info);
    av_log(s, AV_LOG_VERBOSE, "Core version: 0x%x  API version: 0x%x\n", core_info.core, core_info.api);
    av_log(s, AV_LOG_VERBOSE, "Core VS version string: %s\n", core_info.versionString ? core_info.versionString : "(null)");
    av_log(s, AV_LOG_INFO, "Number of Threads: %i\n", core_info.numThreads);
    av_log(s, AV_LOG_INFO, "Max Framebuffer Size: %"PRIi64"kiB\n", core_info.maxFramebufferSize / 1024u);

    if (sz < 0 || sz > vs->max_script_size) {
        if (sz < 0)
            av_log(s, AV_LOG_WARNING, "Could not determine file size.\n");
        sz = vs->max_script_size;
    }

    buf = av_realloc(NULL, sz + 1);
    if (!buf) {
        err = AVERROR(ENOMEM);
        goto done;
    }
    sz = avio_read(pb, buf, sz);

    if (sz < 0) {
        av_log(s, AV_LOG_ERROR, "Could not read script.\n");
        err = sz;
        goto done;
    }

    // Data left means our buffer (the max_script_size option) is too small
    if (avio_read(pb, &dummy, 1) == 1) {
        av_log(s, AV_LOG_ERROR, "File size is larger than max_script_size option "
               "value %"PRIi64", consider increasing the max_script_size option\n",
               vs->max_script_size);
        err = AVERROR_BUFFER_TOO_SMALL;
        goto done;
    }
    buf[sz] = '\0';
    buf = av_realloc_f(buf, sz + 1, sizeof(*buf));
    if (!buf) {
        av_log(s, AV_LOG_ERROR, "Shrinking script buffer to %"PRIi64" failed.\n", sz + 1);
        err = AVERROR(ENOMEM);
        goto done;
    }

    if (vs->vsscriptapi->evaluateBuffer(vss_state->vss, buf, s->url)) {
        const char *msg = vs->vsscriptapi->getError(vss_state->vss);
        av_log(s, AV_LOG_ERROR, "Failed to parse script: %s\n", msg ? msg : "(unknown)");
        err = AVERROR_EXTERNAL;
        goto done;
    }

    vs->num_outputs = 0;
    vs->outputs = NULL;

    for (iout = 0; iout < vs->output_max_probe; ++iout) {
        VSNode *outnode = vs->vsscriptapi->getOutputNode(vss_state->vss, iout);
        if (!outnode)
            continue;

        st = avformat_new_stream(s, NULL);
        if (!st) {
            err = AVERROR(ENOMEM);
            goto done;
        }

        vs->num_outputs++;
        vs->outputs = av_realloc_f(vs->outputs, vs->num_outputs, sizeof(*vs->outputs));
        if (!vs->outputs) {
            err = AVERROR(ENOMEM);
            goto done;
        }
        currout = vs->outputs + vs->num_outputs - 1;

        currout->stream_index = st->index;
        currout->output_index = iout;
        currout->is_constant_rate = 1;
        currout->current_frame = 0;
        currout->node = outnode;
        currout->type = vs->vsapi->getNodeType(outnode);
        if (currout->type == mtVideo) {
            vinfo = vs->vsapi->getVideoInfo(outnode);
            if (!vinfo) {
                av_log(s, AV_LOG_ERROR, "Audio info is NULL for output %i.\n", iout);
                err = AVERROR_BUG;
                goto done;
            }

            if (vinfo->width == 0 || vinfo->height == 0) {
                av_log(s, AV_LOG_ERROR, "Non-constant video format not supported for output %i.\n",
                        iout);
                err = AVERROR_PATCHWELCOME;
                goto done;
            }

            if (vinfo->fpsDen) {
                currout->is_constant_rate = 1;
                avpriv_set_pts_info(st, 64, vinfo->fpsDen, vinfo->fpsNum);
                st->duration = vinfo->numFrames;
                st->nb_frames = vinfo->numFrames;
            } else {
                // VFR. Just set "something".
                currout->is_constant_rate = 0;
                avpriv_set_pts_info(st, 64, 1, AV_TIME_BASE);
                s->ctx_flags |= AVFMTCTX_UNSEEKABLE;
            }

            st->codecpar->codec_type = AVMEDIA_TYPE_VIDEO;
            st->codecpar->codec_id = AV_CODEC_ID_WRAPPED_AVFRAME;
            st->codecpar->width = vinfo->width;
            st->codecpar->height = vinfo->height;
            st->codecpar->format = match_pixfmt(&vinfo->format, currout->c_order);

            vs->vsapi->getVideoFormatName(&vinfo->format, tempbuf);
            if (st->codecpar->format == AV_PIX_FMT_NONE) {
                av_log(s, AV_LOG_ERROR, "Unsupported VS pixel format %s for output %i.\n",
                        tempbuf, iout);
                err = AVERROR_EXTERNAL;
                goto done;
            }
            av_log(s, AV_LOG_VERBOSE, "VS format %s -> pixfmt %s\n", tempbuf,
                   av_get_pix_fmt_name(st->codecpar->format));

            av_log(s, AV_LOG_INFO, "VS output %i (video: %s) -> stream %i:%i.\n",
                  iout, tempbuf, st->id, currout->stream_index);
        } else if (currout->type == mtAudio) {
            ainfo = vs->vsapi->getAudioInfo(outnode);
            if (!ainfo) {
                av_log(s, AV_LOG_ERROR, "Audio info is NULL for output %i.\n", iout);
                err = AVERROR_BUG;
                goto done;
            }

            if (ainfo->format.numChannels >= AV_NUM_DATA_POINTERS) {
                av_log(s, AV_LOG_ERROR, "Too many audio channels (%i >= %i) for output %i\n",
                        ainfo->format.numChannels, AV_NUM_DATA_POINTERS, currout->output_index);
                err = AVERROR_PATCHWELCOME;
                goto done;
            }

            vs->vsapi->getAudioFormatName(&ainfo->format, tempbuf);
            st->codecpar->format = AV_SAMPLE_FMT_NONE;
            if (ainfo->format.sampleType == stInteger) {
                if (ainfo->format.bytesPerSample == sizeof(int16_t)) {
                    st->codecpar->format = AV_SAMPLE_FMT_S16P;
                } else if (ainfo->format.bytesPerSample == sizeof(int32_t)) {
                    st->codecpar->format = AV_SAMPLE_FMT_S32P;
                } else if (ainfo->format.bytesPerSample == sizeof(int64_t)) {
                    st->codecpar->format = AV_SAMPLE_FMT_S64P;
                }
            } else if (ainfo->format.sampleType == stFloat) {
                if (ainfo->format.bytesPerSample == sizeof(float)) {
                    st->codecpar->format = AV_SAMPLE_FMT_FLTP;
                } else if (ainfo->format.bytesPerSample == sizeof(double)) {
                    st->codecpar->format = AV_SAMPLE_FMT_DBLP;
                }
            }
            if (st->codecpar->format == AV_SAMPLE_FMT_NONE) {
                av_log(s, AV_LOG_ERROR, "Unsupported audio format %s (%i) for output %i.\n",
                        tempbuf, ainfo->format.sampleType, iout);
                err = AVERROR_PATCHWELCOME;
                goto done;
            }

            st->codecpar->codec_type = AVMEDIA_TYPE_AUDIO;
            st->codecpar->codec_id = get_pcm_codec(st->codecpar->format);
            st->codecpar->bits_per_raw_sample = ainfo->format.bitsPerSample;
            st->codecpar->bits_per_coded_sample = ainfo->format.bitsPerSample;
            st->codecpar->block_align = ainfo->format.bitsPerSample * ainfo->format.numChannels / 8;
            // TODO: Use ainfo->format.channelLayout
            av_channel_layout_default(&st->codecpar->ch_layout, ainfo->format.numChannels);
            st->codecpar->sample_rate = ainfo->sampleRate;
            st->codecpar->bit_rate = (int64_t)ainfo->sampleRate * ainfo->format.bitsPerSample * ainfo->format.numChannels;
            st->nb_frames = ainfo->numFrames;
            st->duration = ainfo->numSamples;
            avpriv_set_pts_info(st, 64, 1, ainfo->sampleRate);

            av_log(s, AV_LOG_INFO, "VS output %i (audio: %s) -> stream %i:%i.\n",
                  iout, tempbuf, st->id, currout->stream_index);
        } else {
            av_log(s, AV_LOG_WARNING, "Unsupported node type %i at output %i.\n",
                    currout->type, iout);
            vs->num_outputs--;
            // TODO: Free st?
        }
    }

done:
    av_free(buf);
    return err;
}

/**
 * Callback for @ref AVBuffer to free a @ref AVFrame because this demuxer
 * passes AVFrames directly as packet data that also needs proper deallocation.
 *
 * @param opaque Unused.
 * @param data The @ref AVFrame itself.
 */
static void free_frame(void *opaque, uint8_t *data)
{
    AVFrame *frame = (AVFrame *)data;

    av_frame_free(&frame);
}

/**
 * Returns a VapourSynth map property as a int in a safe way with error checking.
 *
 * @param s The main context.
 * @param map The map to pull the property from.
 * @param name The name of the property to pull from the specified map.
 * @oaram def The fallback value if an error occurred (like not found, or
 *            out of range).
 * @return The property value as an int, or the fallback default value if
 *         an error has occurred.
 */
static int get_vs_prop_int(AVFormatContext *s, const VSMap *map, const char *name, int def)
{
    VSContext *vs = s->priv_data;
    int64_t res;
    int err = 1;

    res = vs->vsapi->mapGetInt(map, name, 0, &err);
    return err || res < INT_MIN || res > INT_MAX ? def : res;
}

/**
 * Context object for use inside video @ref AVFrame to keep the
 * frame memory owned by VapourSynth alive (until the @ref AVFrame itself
 * gets freed.).
 */
struct vsframe_ref_data {
    const VSAPI *vsapi;
    const VSFrame *frame;
    AVBufferRef *vss_state;
};

/**
 * Callback for @ref AVBuffer to free the contents of @ref vsframe_ref_data
 * structure for us.
 *
 * @param opaque The @ref vsframe_ref_data structure.
 * @param data Unused.
 */
static void free_vsframe_ref(void *opaque, uint8_t *data)
{
    struct vsframe_ref_data *d = opaque;

    if (d->frame)
        d->vsapi->freeFrame(d->frame);

    av_buffer_unref(&d->vss_state);

    av_free(d);
}

/**
 * Reads and fills a frame of video into the specified packet.
 * The packet will contain a already-prepared @ref AVFrame.
 */
static int read_packet_video(AVFormatContext *s, AVPacket *pkt, struct VSOutput *currout)
{
    VSContext *vs = s->priv_data;
    AVStream *st = s->streams[currout->stream_index];
    AVFrame *frame = NULL;
    char vserr[80];
    const VSFrame *vsframe;
    const VSVideoInfo *info = vs->vsapi->getVideoInfo(currout->node);
    const VSMap *props;
    const AVPixFmtDescriptor *desc;
    AVBufferRef *vsframe_ref = NULL;
    struct vsframe_ref_data *ref_data;
    int err = 0;
    int i;

    if (currout->current_frame >= info->numFrames)
        return AVERROR_EOF;

    ref_data = av_mallocz(sizeof(*ref_data));
    if (!ref_data) {
        err = AVERROR(ENOMEM);
        goto end;
    }

    // The READONLY flag is important because the ref is reused for plane data
    vsframe_ref = av_buffer_create(NULL, 0, free_vsframe_ref, ref_data, AV_BUFFER_FLAG_READONLY);
    if (!vsframe_ref) {
        err = AVERROR(ENOMEM);
        av_free(ref_data);
        goto end;
    }

    vsframe = vs->vsapi->getFrame(currout->current_frame, currout->node, vserr, sizeof(vserr));
    if (!vsframe) {
        av_log(s, AV_LOG_ERROR, "Error getting video frame %i for output %i: %s\n",
                currout->current_frame, currout->output_index, vserr);
        err = AVERROR_EXTERNAL;
        goto end;
    }
    av_assert0(vs->vsapi->getFrameType(vsframe) == mtVideo);

    ref_data->vsapi = vs->vsapi;
    ref_data->frame = vsframe;

    ref_data->vss_state = av_buffer_ref(vs->vss_state);
    if (!ref_data->vss_state) {
        err = AVERROR(ENOMEM);
        goto end;
    }

    props = vs->vsapi->getFramePropertiesRO(vsframe);

    frame = av_frame_alloc();
    if (!frame) {
        err = AVERROR(ENOMEM);
        goto end;
    }

    frame->format       = st->codecpar->format;
    frame->width        = st->codecpar->width;
    frame->height       = st->codecpar->height;
    frame->colorspace   = st->codecpar->color_space;

    // Values according to ISO/IEC 14496-10.
    frame->colorspace       = get_vs_prop_int(s, props, "_Matrix",      frame->colorspace);
    frame->color_primaries  = get_vs_prop_int(s, props, "_Primaries",   frame->color_primaries);
    frame->color_trc        = get_vs_prop_int(s, props, "_Transfer",    frame->color_trc);

    if (get_vs_prop_int(s, props, "_ColorRange", 1) == 0)
        frame->color_range = AVCOL_RANGE_JPEG;

    frame->sample_aspect_ratio.num = get_vs_prop_int(s, props, "_SARNum", 0);
    frame->sample_aspect_ratio.den = get_vs_prop_int(s, props, "_SARDen", 1);

    av_assert0(vs->vsapi->getFrameWidth(vsframe, 0) == frame->width);
    av_assert0(vs->vsapi->getFrameHeight(vsframe, 0) == frame->height);

    desc = av_pix_fmt_desc_get(frame->format);

    for (i = 0; i < FFMIN(info->format.numPlanes, AV_NUM_DATA_POINTERS); i++) {
        int p = currout->c_order[i];
        ptrdiff_t plane_h = frame->height;

        frame->data[i] = (void *)vs->vsapi->getReadPtr(vsframe, p);
        frame->linesize[i] = vs->vsapi->getStride(vsframe, p);

        frame->buf[i] = av_buffer_ref(vsframe_ref);
        if (!frame->buf[i]) {
            err = AVERROR(ENOMEM);
            goto end;
        }

        // Each plane needs an AVBufferRef that indicates the correct plane
        // memory range. VapourSynth doesn't even give us the memory range,
        // so make up a bad guess to make FFmpeg happy (even if almost nothing
        // checks the memory range).
        if (i == 1 || i == 2)
            plane_h = AV_CEIL_RSHIFT(plane_h, desc->log2_chroma_h);
        frame->buf[i]->data = frame->data[i];
        frame->buf[i]->size = frame->linesize[i] * plane_h;
    }

    pkt->buf = av_buffer_create((uint8_t*)frame, sizeof(*frame),
                                free_frame, NULL, 0);
    if (!pkt->buf) {
        err = AVERROR(ENOMEM);
        goto end;
    }
    frame = NULL; // pkt owns it now

    pkt->data   = pkt->buf->data;
    pkt->size   = pkt->buf->size;
    pkt->flags |= AV_PKT_FLAG_TRUSTED;
    pkt->stream_index = currout->stream_index;

    if (currout->is_constant_rate)
        pkt->pts = currout->current_frame;

    currout->current_frame++;

end:
    av_frame_free(&frame);
    av_buffer_unref(&vsframe_ref);
    return err;
}

/**
 * Reads and fills a frame of audio into the specified packet.
 * The packet will contain PCM data.
 */
static int read_packet_audio(AVFormatContext *s, AVPacket *pkt, struct VSOutput *currout)
{
    VSContext *vs = s->priv_data;
    AVStream *st = s->streams[currout->stream_index];
    char vserr[80];
    const VSFrame *vsframe;
    const VSAudioInfo *info = vs->vsapi->getAudioInfo(currout->node);
    int err = 0;
    int i;
    int plane_size;
    uint8_t *pcm_data;
    size_t pcm_size;

    if (currout->current_frame >= info->numFrames)
        return AVERROR_EOF;

    vsframe = vs->vsapi->getFrame(currout->current_frame, currout->node, vserr, sizeof(vserr));
    if (!vsframe) {
        av_log(s, AV_LOG_ERROR, "Error getting audio frame %i for output %i: %s\n",
                currout->current_frame, currout->output_index, vserr);
        err = AVERROR_EXTERNAL;
        goto end;
    }
    av_assert0(vs->vsapi->getFrameType(vsframe) == mtAudio);

    plane_size = vs->vsapi->getFrameLength(vsframe) * info->format.bytesPerSample;
    pcm_size = info->format.numChannels * plane_size;
    pcm_data = av_mallocz(pcm_size);
    if (!pcm_data) {
        err = AVERROR(ENOMEM);
        goto end;
    }

    if (is_pcm_codec_planar(st->codecpar->codec_id)) {
        uint8_t *write_ptr = pcm_data;
        for (i = 0; i < info->format.numChannels; i++, write_ptr += plane_size) {
            /* currout->c_order unused here because we never set it anyway. */
            const uint8_t *read_ptr = vs->vsapi->getReadPtr(vsframe, i);
            memcpy(write_ptr, read_ptr, plane_size);
        }
    } else {
        int bytes_per_sample = info->format.bytesPerSample;
        int num_channels = info->format.numChannels;
        int stride = num_channels * bytes_per_sample;
        // Not the best but does the job
        for (i = 0; i < num_channels; i++) {
            /* currout->c_order unused here because we never set it anyway. */
            const uint8_t *read_ptr = vs->vsapi->getReadPtr(vsframe, i);
            const uint8_t *read_end = read_ptr + plane_size;
            uint8_t *write_ptr = pcm_data + i * bytes_per_sample;
            const uint8_t *write_end = write_ptr + plane_size;
            for (; write_ptr < write_end && read_ptr < read_end;
                    write_ptr += stride, read_ptr += bytes_per_sample)
                memcpy(write_ptr, read_ptr, bytes_per_sample);
            // TODO: make them av_assert1
            av_assert0(write_ptr == write_end);
            av_assert0(read_ptr == read_end);
        }
    }

    pkt->time_base = st->time_base;
    pkt->duration = vs->vsapi->getFrameLength(vsframe);

    pkt->buf = av_buffer_create(pcm_data, pcm_size, av_buffer_default_free, NULL, 0);
    if (!pkt->buf) {
        err = AVERROR(ENOMEM);
        av_free(pcm_data);
        goto end;
    }
    pkt->data = pkt->buf->data;
    pkt->size = pkt->buf->size;
    pkt->stream_index = currout->stream_index;

    if (currout->is_constant_rate)
        pkt->pts = currout->current_frame * VS_AUDIO_FRAME_SAMPLES;

    currout->current_frame++;

end:
    vs->vsapi->freeFrame(vsframe);
    return err;
}

/**
 * Callback to read an packet of data.
 *
 * Currently, on each call it'll go to the "next" stream and pulls a frame
 * of data to pass.
 */
static int read_packet_vs(AVFormatContext *s, AVPacket *pkt)
{
    VSContext *vs = s->priv_data;
    int num_eofs = 0;
    int err = AVERROR_EOF;

    // Switch between the output streams so all streams output data.
    // If one goes EOF, then it'll be ignored until all streams are EOF.
    // Also correctly handles the case when there are no outputs.
    // TODO: Consider outputting in such a way that both streams always stay synchronized.
    while (err == AVERROR_EOF && num_eofs != vs->num_outputs) {
        struct VSOutput *currout = vs->outputs + vs->current_output;

        if (s->streams[currout->stream_index]->discard != AVDISCARD_ALL) {
            if (currout->type == mtVideo) {
                err = read_packet_video(s, pkt, currout);
            } else if (currout->type == mtAudio) {
                err = read_packet_audio(s, pkt, currout);
            } else {
                av_log(s, AV_LOG_ERROR, "num_output=%i/%i was out of bounds,"
                        " this is an internal bug, please report.\n",
                        vs->num_outputs, vs->num_outputs - 1);
                err = AVERROR_BUG;
            }
        }
        // If DISCARD_ALL, then err stays AVERROR_EOF, so this works out

        if (err == AVERROR_EOF)
            num_eofs++;

        vs->current_output++;
        vs->current_output %= vs->num_outputs;
    }

    return err;
}

/**
 * Callback to seek all streams to an specified time stamp.
 * While the streams are independent of each other and thus seekable
 * independent of each other, this isn't supported here in libavformat,
 * so it seeks all streams.
 */
static int read_seek_vs(AVFormatContext *s, int stream_idx, int64_t ts, int flags)
{
    VSContext *vs = s->priv_data;
    const AVStream *selected = s->streams[stream_idx];
    double time;
    int i;

    for (i = 0; i < vs->num_outputs; i++)
        if (!vs->outputs[i].is_constant_rate)
            return AVERROR(ENOSYS);

    time = (double)selected->time_base.num / selected->time_base.den * ts;

    for (i = 0; i < vs->num_outputs; i++) {
        struct VSOutput *currout = vs->outputs + i;
        const AVStream *st = s->streams[currout->stream_index];
        if (currout->type == mtVideo) {
            double rate = (double)st->time_base.den / st->time_base.num;
            currout->current_frame = rate * time;
        } else if (currout->type == mtAudio) {
            double rate = (double)st->codecpar->sample_rate / VS_AUDIO_FRAME_SAMPLES;
            currout->current_frame = rate * time;
        }
        currout->current_frame = FFMIN(FFMAX(0, currout->current_frame),
                s->streams[currout->stream_index]->nb_frames);
    }

    return 0;
}

/**
 * Callback to probe whenever the data is suitable for this codec.
 * Since the scripts in VapourSynth are Python scripts without any sandbox,
 * it would run arbitrary code that could do harm on the user's system.
 * As such, it'll do nothing.
 */
static av_cold int probe_vs(const AVProbeData *p)
{
    // Explicitly do not support this. VS scripts are written in Python, and
    // can run arbitrary code on the user's system.
    return 0;
}

static const AVClass class_vs = {
    .class_name = "VapourSynth demuxer",
    .item_name  = av_default_item_name,
    .option     = options,
    .version    = LIBAVUTIL_VERSION_INT,
};

const FFInputFormat ff_vapoursynth_demuxer = {
    .p.name         = "vapoursynth",
    .p.long_name    = NULL_IF_CONFIG_SMALL("VapourSynth demuxer"),
    .p.priv_class   = &class_vs,
    .p.flags        = AVFMT_NO_BYTE_SEEK,
    .priv_data_size = sizeof(VSContext),
    .flags_internal = FF_INFMT_FLAG_INIT_CLEANUP,
    .read_probe     = probe_vs,
    .read_header    = read_header_vs,
    .read_packet    = read_packet_vs,
    .read_close     = read_close_vs,
    .read_seek      = read_seek_vs,
};
