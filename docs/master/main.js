
function scrollIntoViewIfNeeded(target) { 
    if (target.getBoundingClientRect().bottom > window.innerHeight) {
        target.scrollIntoView(false);
    }

    if (target.getBoundingClientRect().top < 0) {
        target.scrollIntoView();
    } 
}
function selectText(element) {
    if (document.selection) { // IE
        var range = document.body.createTextRange();
        range.moveToElementText(element);
        range.select();
    } else if (window.getSelection) {
        var range = document.createRange();
        range.selectNode(element);
        window.getSelection().removeAllRanges();
        window.getSelection().addRange(range);
    }
}
function toggleExamples(examples, relativeTo) {
    // Since this changes the examples shown on the entire page, it has the potential to
    // really disorient the user as the content of the page will shift and potentially
    // scroll what they're looking away or even off screen. So we keep track of the y position
    //  "target" element (relative to the window) at the start, and then adjust the scroll
    // of the window after to make sure what they clicked on stays in the same spot.
    if (relativeTo) {
        var startTop = relativeTo[0].getBoundingClientRect().top;
    }
    $('.ex-tab-content').hide();
    $('.ex-tab-content.' + examples).show();
    $('.ex-tab').removeClass('selected');
    $('.ex-tab[target=' + examples + ']').addClass('selected');
    if (relativeTo) {
        var deltaTop = relativeTo[0].getBoundingClientRect().top - startTop;
        window.scrollBy(0, deltaTop);
    }
}
$(document).ready(function() {
    $('.ds-ref').click(function () {
        var target = $('[id="' + $(this).text() + '"]');
        target.effect("highlight", {}, 1000);
        scrollIntoViewIfNeeded(target[0]);
    });
    var left = 0, top = 0;
    $(document).on('mousedown', '.select', function(e) {left = e.pageX; top = e.pageY;});
    $(document).on('mouseup', '.select', function(e) { if (left == e.pageX && top == e.pageY) {selectText(this);} });
    $(document).on('click', '.jumpto', function(e) {
        var target = $('[id="' + $(e.target).attr('href').substr(1) + '"]');
        target.effect("highlight", {}, 1000);
        scrollIntoViewIfNeeded(target[0]);
        return false;
    });
    $('.tabs').each(function (i, e) {
        var first = $(e).find('.tab-content:first');
        $(e).find('.tab-content').hide();
        first.show();
        $(e).find('.tab:first').addClass('selected');
        $(e).find('.tab').prependTo(e);
    });
    $(document).on('click', '.tab', function(e) {
        var $target = $(e.target);
        var tabs = $target.closest('.tabs');
        tabs.find('.tab-content').hide();
        $('[id="'+$target.attr('target')+'"]').show();
        tabs.find('.tab.selected').removeClass('selected');
        $target.addClass('selected');
    });
    $('#DatasetJump').change(function () {
        var targetRow = $('#DatasetJump').val();
        if (targetRow) {
            $('#' + targetRow)[0].scrollIntoView();
            $('#DatasetJump').val(''); // clear selection
        }
    });
    $('.ex-tabs').each(function (i, e) {
        $(e).find('.ex-tab').prependTo(e);
    });
    var examples = null;
    if (window.sessionStorage) {
        examples = sessionStorage.getItem("examples");
    }
    if (!examples && window.localStorage) {
        examples = localStorage.getItem("examples");
    }
    if (!examples) {
        examples = 'irds-python';
    }
    toggleExamples(examples, null);
    $(document).on('click', '.ex-tab', function(e) {
        var $target = $(e.target);
        var examples = $target.attr('target');
        if (window.sessionStorage) {
            sessionStorage.setItem("examples", examples);
        }
        if (window.localStorage) {
            localStorage.setItem("examples", examples);
        }
        toggleExamples(examples, $target);
    });
    $(document).on('mouseenter', '[data-highlight]', function(e) {
        var $target = $(e.target);
        var hlId = $target.attr('data-highlight');
        $(document.getElementById(hlId)).addClass('hl');
    });
    $(document).on('mouseleave', '[data-highlight]', function(e) {
        var $target = $(e.target);
        var hlId = $target.attr('data-highlight');
        $(document.getElementById(hlId)).removeClass('hl');
    });
});
function toEmoji(test, result) {
    if (test) {
        if (result === 'FAIL_BUT_HAS_MIRROR') {
            return '❎';
        } else {
            return '✅';
        }
    }
    return '❌';
}
function toTime(duration) {
    if (duration < 60) {
        return duration.toFixed(2) + 's';
    }
    var minutes = Math.floor(duration / 60);
    var seconds = duration % 60;
    return minutes.toFixed(0) + 'm ' + seconds.toFixed(0) + 's';
}
function toFileSize(size) {
    if (!size) {
        return '';
    }
    var unit = 'B';
    var units = ['KB', 'MB', 'GB'];
    while (units.length > 0 && size > 1000) {
        size = size / 1000;
        unit = units.shift();
    }
    if (unit === 'B') {
        size = size.toFixed(0);
    } else {
        size = size.toFixed(1);
    }
    return size + ' ' + unit;
}
function generateDownloads(title, downloads) {
    if (downloads.length === 0) {
        return $('<div></div>');
    }
    var allGood = true;
    var $content = $('<table></table>');
    $content.append($('<tr><th>Avail</th><th>Download ID</th><th>Size</th><th>Time</th><th>Last Tested At</th><th>Expected MD5 Hash</th></tr>'));
    var goodCount = 0;
    var totalCount = 0;
    $.each(downloads, function (i, dl) {
        var good = dl.result === "PASS" || dl.result === 'FAIL_BUT_HAS_MIRROR';
        totalCount += 1;
        if (!good) {
            allGood = false;
        } else {
            goodCount += 1;
        }
        $content.append($('<tr></tr>')
            .append($('<td></td>').text(toEmoji(good, dl.result)).attr('title', dl.result).css('text-align', 'center'))
            .append($('<td></td>').append($('<a></a>').attr('href', dl.url).text(dl.name)))
            .append($('<td></td>').text(toFileSize(dl.size)))
            .append($('<td></td>').text(toTime(dl.duration)))
            .append($('<td></td>').text(dl.time.substring(0, 19).replace('T', ' ')))
            .append($('<td></td>').append($('<code>').text(dl.md5)))
        );
    });
    return $('<details></details>')
        .append($('<summary></summary>').text(toEmoji(allGood) + ' ' + title + ' (' + goodCount.toString() + ' of ' + totalCount.toString() + ')'))
        .append($('<p>These files are automatically downloaded by ir_datasets as they are needed. We also periodically check that they are still available and unchanged through an automated <a href="https://github.com/allenai/ir_datasets/actions/workflows/verify_downloads.yml">GitHub action</a>. The latest results from that test are shown here:</p>'))
        .append($content)
        .prop('open', !allGood);
}
