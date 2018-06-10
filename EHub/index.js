/* -----------------------------------------------------------------------------
 * @copyright (C) 2017, Alert Logic, Inc
 * @doc
 *
 * The purpose of this function it to be registered as an O365 webhook and
 * receive/process notifications.
 * https://msdn.microsoft.com/en-us/office-365/office-365-management-activity-api-reference#receiving-notifications
 *
 * @end
 * -----------------------------------------------------------------------------
 */

const m_o365content = require('./o365content');


module.exports = function (context, eventHubMessages) {
    var messages = eventHubMessages.records;
    
    return m_o365content.processContent(context, messages,
        function(err) {
            if (err) {
                context.log.error(`${err}`);
                context.res.headers = {};
                context.res.status = 500;
                context.done(err);
            } else {
                context.log.info('OK!');
                context.done();
            }
        });
};
