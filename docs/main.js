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
$(document).ready(function() {
    $('.showhide').click(function () {
        var query = '[data-parent]';
        if (!$(this).is('.all')) {
            query = '[data-parent="' + $(this).closest('.dataset').attr('id') + '"]';
        }
        var isHidden = $(this).hasClass('hidden');
        $(query).toggle(isHidden);
        $(this).toggleClass('shown hidden');
        if ($(this).is('.all')) {
            $('.showhide').toggleClass('shown', isHidden);
            $('.showhide').toggleClass('hidden', !isHidden);
        }
    });
    $('.showhide.all').click();
    $('.ds-ref').click(function () {
        var target = $('[id="' + $(this).text() + '"]');
        target.effect("highlight", {}, 1000);
        scrollIntoViewIfNeeded(target[0]);
    });
    $('.tag[data-fields]').click(function() {
        var dsName = $(this).closest('.dataset').find('.ds-name').text();
        var fields = $(this).attr('data-fields');
        var type = $(this).text();
        $('#CodeSample').html('<code class="select"><div><span class="kwd">import</span> ir_datasets</div><div>dataset = ir_datasets.load(<span class="str">'+dsName+')</div><div><span class="kwd">for</span> ' + fields + ' <span class="kwd">in</span> dataset.' + type + '_iter():</div><div>&nbsp;&nbsp;&nbsp;&nbsp;<span class="kwd">pass</span></div></code>');
        $('#Popup,#Backdrop').show();
    });
    $('#Backdrop,#ClosePopup').click(function() {
        $('#Popup,#Backdrop').hide();
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
});
